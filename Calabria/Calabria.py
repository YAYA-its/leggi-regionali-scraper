import os
import json
import re
import requests
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pypdf import PdfReader
from io import BytesIO

# ==================================================
# CONFIG
# ==================================================
REGION = "Calabria"
BASE_API = "https://www.consiglioregionale.calabria.it/bdf/api/BDF"

START_YEAR = 1971
END_YEAR = 2025

MAX_WORKERS = 8
MAX_CONSECUTIVE_MISSES = 10  # auto-stop per year

OUTPUT_DIR = "Calabria_Laws"
PDF_DIR = os.path.join(OUTPUT_DIR, "pdfs")
EXCEL_PATH = os.path.join(OUTPUT_DIR, "Calabria_Laws_Index.xlsx")
PROGRESS_FILE = os.path.join(OUTPUT_DIR, "progress.json")

os.makedirs(PDF_DIR, exist_ok=True)

# ==================================================
# LOAD / INIT PROGRESS
# ==================================================
if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
        state = json.load(f)
else:
    state = {
        "done_keys": [],   # "year_lawnum"
        "done_years": []
    }

def save_progress():
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

# ==================================================
# HELPERS
# ==================================================
def italian_date_to_iso(date_text):
    try:
        return datetime.strptime(date_text, "%d %B %Y").strftime("%Y-%m-%d")
    except:
        return None

def extract_date_from_pdf(pdf_bytes):
    try:
        reader = PdfReader(BytesIO(pdf_bytes))
        text = reader.pages[0].extract_text().lower()
        match = re.search(r"\d{1,2}\s+[a-zà]+\s+\d{4}", text)
        return match.group(0) if match else None
    except:
        return None

def extract_title_from_pdf(pdf_bytes):
    """
    Extract real law title from first page
    """
    try:
        reader = PdfReader(BytesIO(pdf_bytes))
        text = reader.pages[0].extract_text()
        lines = [l.strip() for l in text.splitlines() if l.strip()]

        for line in lines:
            low = line.lower()
            if low.startswith("legge regionale"):
                continue
            if len(line) > 20:
                return line

        return ""
    except:
        return ""

# ==================================================
# FETCH PDF
# ==================================================
def fetch_pdf(year, law_num):
    key = f"{year}_{law_num}"
    if key in state["done_keys"]:
        return None, False

    url = f"{BASE_API}?numero={law_num}&anno={year}"

    try:
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            return None, False

        # ✅ reliable PDF detection
        if not r.content.startswith(b"%PDF"):
            return None, False

        # ---------- DATE ----------
        date_text = None

        cd = r.headers.get("Content-Disposition", "").lower()
        m = re.search(r"\d{1,2}\s+[a-zà]+\s+\d{4}", cd)
        if m:
            date_text = m.group(0)

        if not date_text:
            date_text = extract_date_from_pdf(r.content)

        iso_date = italian_date_to_iso(date_text) if date_text else None
        if not iso_date:
            iso_date = f"{year}-01-01"
            date_text = str(year)

        # ---------- TITLE ----------
        title = extract_title_from_pdf(r.content)
        if not title:
            title = f"Legge Regionale n. {law_num}"

        filename = f"{REGION}_{law_num}_{iso_date}.pdf"
        pdf_path = os.path.join(PDF_DIR, filename)

        if not os.path.exists(pdf_path):
            with open(pdf_path, "wb") as f:
                f.write(r.content)

        state["done_keys"].append(key)

        return {
            "Region": REGION,
            "Law Title": title,
            "Law Number": law_num,
            "Date": date_text,
            "Filename": filename
        }, True

    except Exception as e:
        print(f"⚠ {year} n.{law_num}: {e}")
        return None, False

# ==================================================
# MAIN (DYNAMIC PER YEAR)
# ==================================================
for year in range(START_YEAR, END_YEAR + 1):

    if year in state["done_years"]:
        print(f"⏭ Skipping {year}")
        continue

    print(f"\n📅 Processing year {year}")

    year_records = []
    law_num = 1
    consecutive_misses = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}

        while consecutive_misses < MAX_CONSECUTIVE_MISSES:
            futures[executor.submit(fetch_pdf, year, law_num)] = law_num
            law_num += 1

            if len(futures) >= MAX_WORKERS:
                for future in as_completed(futures):
                    result, found = future.result()
                    if found:
                        year_records.append(result)
                        consecutive_misses = 0
                    else:
                        consecutive_misses += 1
                    futures.pop(future)
                    break

        # Drain remaining
        for future in as_completed(futures):
            result, found = future.result()
            if found:
                year_records.append(result)

    # ---------- SAVE EXCEL (PHASE END) ----------
    if year_records:
        df_new = pd.DataFrame(year_records)
        if os.path.exists(EXCEL_PATH):
            df_old = pd.read_excel(EXCEL_PATH)
            df_new = pd.concat([df_old, df_new], ignore_index=True)
        df_new.to_excel(EXCEL_PATH, index=False)

    print(f"📊 {year}: {len(year_records)} PDFs (exact)")

    state["done_years"].append(year)
    save_progress()

# ==================================================
# EXACT COUNT FROM DISK
# ==================================================
def count_pdfs_by_year(pdf_dir):
    counts = {}
    for f in os.listdir(pdf_dir):
        if f.lower().endswith(".pdf"):
            m = re.search(r"_(\d{4})-\d{2}-\d{2}\.pdf$", f)
            if m:
                y = int(m.group(1))
                counts[y] = counts.get(y, 0) + 1
    return counts

print("\n📊 FINAL EXACT COUNTS (FROM DISK)")
print("-" * 40)

counts = count_pdfs_by_year(PDF_DIR)
for y in range(START_YEAR, END_YEAR + 1):
    print(f"{y}: {counts.get(y, 0)} PDFs")

print("\n✅ COMPLETED SUCCESSFULLY")
print(f"📂 PDFs → {PDF_DIR}")
print(f"📊 Excel → {EXCEL_PATH}")
