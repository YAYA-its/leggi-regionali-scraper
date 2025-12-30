import os
import re
import json
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from threading import Lock

# ============================================================
# CONFIG
# ============================================================
BASE_URL = "https://www.cr.campania.it"
LISTING_URL = BASE_URL + "/leggi-progetti/leggi-regolamenti"

REGION = "Campania"
OUTPUT_DIR = "Campania_Laws"
PDF_DIR = os.path.join(OUTPUT_DIR, "pdfs")
EXCEL_FILE = os.path.join(OUTPUT_DIR, "Campania_Laws.xlsx")
STATE_FILE = os.path.join(OUTPUT_DIR, "state.json")

MAX_WORKERS = 4
MAX_RETRIES = 3
RETRY_DELAY = 3

# ✅ 1. SETUP SESSION (MOHIM BESAF BACH TFOUT PAGE 50)
session = requests.Session()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive"
}

session.headers.update(HEADERS)

os.makedirs(PDF_DIR, exist_ok=True)

# ============================================================
# STATE & STATS
# ============================================================
stats = defaultdict(int)
stats_lock = Lock()

if os.path.exists(STATE_FILE):
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        state = json.load(f)
else:
    state = {"done_ids": []}

def save_state():
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)

# ============================================================
# HELPERS
# ============================================================
def get_soup(url):
    # ✅ 2. STA3MEL SESSION.GET
    r = session.get(url, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def clean_filename(text):
    return re.sub(r'[\\/*?:"<>|]', "_", text)

def make_absolute(url):
    return url if url.startswith("http") else BASE_URL + url

# ============================================================
# STEP 1: COLLECT DETAIL LINKS (FORCED RANGE LOOP)
# ============================================================
def collect_detail_links():
    links = []
    
    print("🔄 Starting scan with Session (Forcing range 1-150)...")
    
    empty_streak = 0
    
    with tqdm(
        desc="🔍 Scanning listing pages",
        unit="page",
        dynamic_ncols=True
    ) as pbar:

        # ✅ 3. LOOP FORCEE (Mn 1 tal 150)
        # Haka kan-forcer server y3tina pages li mor 50
        for page in range(1, 150):
            url = f"{LISTING_URL}?page={page}"
            
            try:
                soup = get_soup(url)
                
                # Jma3 les liens
                cards = soup.select("a[href*='dettaglio-documento']")

                if not cards:
                    # Ila page khawya
                    empty_streak += 1
                    # Ila l9ina 3 pages khawyin mtab3in, safi n7ebso
                    if empty_streak >= 3:
                        break
                else:
                    # Ila l9ina data, n-resetiw counter
                    empty_streak = 0

                for a in cards:
                    href = a.get("href")
                    if href:
                        links.append(make_absolute(href))

                pbar.update(1)
                
                # Pause sghira
                time.sleep(0.3)
                
            except Exception as e:
                print(f"⚠️ Error scanning page {page}: {e}")
                # Ila kan error connection, n3awdo njerbo
                continue

    # N7eydo duplicates ila kanou
    return list(dict.fromkeys(links))

# ============================================================
# DOWNLOAD PDF WITH RETRY
# ============================================================
def download_pdf_with_retry(pdf_url, pdf_path):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(pdf_url, timeout=60) # Use session here too
            r.raise_for_status()
            with open(pdf_path, "wb") as f:
                f.write(r.content)
            return True
        except Exception:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    return False

# ============================================================
# PROCESS SINGLE LAW
# ============================================================
def process_law(detail_url):
    try:
        law_id_match = re.search(r"id=(\d+)", detail_url)
        if not law_id_match:
            return None
        law_id = law_id_match.group(1)

        if law_id in state["done_ids"]:
            with stats_lock:
                stats["skipped"] += 1
            return None

        soup = get_soup(detail_url)

        h1 = soup.select_one("h1")
        if not h1:
            return None
        title = h1.get_text(strip=True)

        text_block = soup.get_text("\n", strip=True)

        # Robust law number detection
        num_match = re.search(r"n\.\s*(\d+)|,\s*n\.\s*(\d+)", text_block, re.I)
        if not num_match:
            return None
        law_number = next(g for g in num_match.groups() if g)

        # Clean date detection
        date_match = re.search(
            r"(\d{1,2}\s+[a-zàèìòù]+\s+\d{4})",
            text_block,
            re.I
        )
        if not date_match:
            return None
        law_date = date_match.group(1)

        pdf_a = soup.select_one("a[href*='prendiDocumento']")
        if not pdf_a:
            return None
        pdf_url = make_absolute(pdf_a["href"])

        filename = clean_filename(f"{REGION}_{law_number}_{law_date}.pdf")
        pdf_path = os.path.join(PDF_DIR, filename)

        if not download_pdf_with_retry(pdf_url, pdf_path):
            with stats_lock:
                stats["failed"] += 1
            return None

        with stats_lock:
            stats["downloaded"] += 1

        state["done_ids"].append(law_id)
        save_state()

        return {
            "Region": REGION,
            "Law Title": title,
            "Law Number": law_number,
            "Date": law_date,
            "Filename": filename
        }

    except Exception:
        with stats_lock:
            stats["failed"] += 1
        return None

# ============================================================
# MAIN
# ============================================================
def main():
    print("\n🚀 Campania Laws Scraper Started\n")

    detail_links = collect_detail_links()
    print(f"\n📄 Total laws found: {len(detail_links)}\n")

    rows = []
    if os.path.exists(EXCEL_FILE):
        rows = pd.read_excel(EXCEL_FILE).to_dict("records")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_law, url) for url in detail_links]

        with tqdm(
            total=len(futures),
            desc="⬇ Downloading PDFs",
            unit="file",
            dynamic_ncols=True
        ) as pbar:

            for future in as_completed(futures):
                result = future.result()
                if result:
                    rows.append(result)
                    pd.DataFrame(rows).to_excel(EXCEL_FILE, index=False)

                pbar.update(1)
                pbar.set_postfix(
                    Downloaded=stats["downloaded"],
                    Skipped=stats["skipped"],
                    Failed=stats["failed"]
                )

    print("\n✅ COMPLETED")
    print(f"📥 Downloaded: {stats['downloaded']}")
    print(f"⏭ Skipped: {stats['skipped']}")
    print(f"❌ Failed: {stats['failed']}")

if __name__ == "__main__":
    main()