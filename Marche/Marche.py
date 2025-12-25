import os
import re
import time
import queue
import threading
import base64
import requests
import pandas as pd
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


BASE_URL = "https://www.consiglio.marche.it/banche_dati_e_documentazione/leggi/"
START_URL = BASE_URL + "classificazioni.php?arc=vig"
REGION = "Marche"

ROOT_FOLDER = "Marche"
os.makedirs(ROOT_FOLDER, exist_ok=True)

EXCEL_PATH = os.path.join(ROOT_FOLDER, "Marche_All_Laws.xlsx")

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
})

download_queue = queue.Queue()
results_lock = threading.Lock()

downloaded = 0
failed = 0
skipped = 0
stop_signal = False

all_rows = []


# ----------------- CHROME DRIVER -----------------
def chrome_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )

    driver.execute_cdp_cmd("Page.enable", {})
    return driver


# ----------------- DATA EXTRACTION -----------------
def extract_law_data(detail_url):
    try:
        r = session.get(detail_url, timeout=10)
        if r.status_code != 200:
            return None

        html = r.text

        # ---------- TITLE ----------
        title_match = re.search(r"<td>\s*([^<]*?)\s*</td>\s*</tr>", html, re.S)
        if title_match:
            title = title_match.group(1).strip()
        else:
            h1 = re.search(r"<h1.*?>(.*?)</h1>", html, re.S)
            title = re.sub("<.*?>", "", h1.group(1)).strip() if h1 else "NA"

        # ---------- LAW NUMBER ----------
        num_match = re.search(r"n\.\s?(\d+)", html, re.IGNORECASE)
        law_number = num_match.group(1) if num_match else "NA"

        # ---------- DATE ----------
        date_match = re.search(
            r"(\d{1,2}\s+(gennaio|febbraio|marzo|aprile|maggio|giugno|luglio|agosto|settembre|ottobre|novembre|dicembre)\s+\d{4})",
            html,
            re.IGNORECASE
        )

        clean_date = date_match.group(1) if date_match else "NA"
        safe_date = clean_date.replace(" ", "-")

        filename = f"{REGION}_{law_number}_{safe_date}.pdf"
        filepath = os.path.join(ROOT_FOLDER, filename)

        return {
            "region": REGION,
            "title": title,
            "law_number": law_number,
            "date": clean_date,
            "file": filename,
            "url": detail_url,
            "path": filepath
        }
    except Exception:
        return None


# ----------------- SAVE EXCEL (FIXED) -----------------
def save_excel():
    with results_lock:
        if not all_rows: return
        try:
            df = pd.DataFrame(all_rows)
            df.drop_duplicates(subset=["file"], inplace=True)
            df.to_excel(EXCEL_PATH, index=False)
        except PermissionError:
            # 👇 HNA L-FIX: Ila kan locked, ghir affiche warning w kmmel
            print(f"   ⚠️ Warning: Excel file is OPEN or LOCKED. Skipping save for now.")
        except Exception as e:
            print(f"   ⚠️ Excel Save Error: {e}")


# ----------------- PDF WORKER -----------------
def print_pdf_worker(worker_id):
    global downloaded, failed, skipped

    driver = chrome_driver()

    while True:
        try:
            law = download_queue.get(timeout=3)
        except queue.Empty:
            if stop_signal:
                break
            continue

        if os.path.exists(law["path"]):
            skipped += 1
            # print(f"Skipped (already exists): {law['file']}") # Commented to reduce noise
            download_queue.task_done()
            continue

        try:
            driver.get(law["url"])

            # Wait for content
            time.sleep(0.5)

            pdf = driver.execute_cdp_cmd("Page.printToPDF", {
                "printBackground": True
            })

            with open(law["path"], "wb") as f:
                f.write(base64.b64decode(pdf["data"]))

            downloaded += 1
            print(f"Saved PDF: {law['file']}")

        except Exception as e:
            failed += 1
            print(f"Failed PDF: {law['url']}   Error: {e}")

        download_queue.task_done()

    driver.quit()


# ----------------- CATEGORY SCRAPER -----------------
def process_category(category_url, category_name):
    print(f"\nProcessing Category: {category_name}")

    try:
        rr = session.get(category_url, timeout=15)
        pages = [category_url]

        pagination = re.findall(r'href="(classificazioni\.php\?[^"]+page=\d+)"', rr.text)
        for p in pagination:
            full_p = BASE_URL + p
            if full_p not in pages:
                pages.append(full_p)

        for page_url in pages:
            r = session.get(page_url, timeout=15)
            detail_links = re.findall(r'href="(dettaglio\.php\?arc=vig[^"]+)"', r.text)

            for d in detail_links:
                full = BASE_URL + d
                law = extract_law_data(full)

                if law:
                    with results_lock:
                        all_rows.append(law)

                    download_queue.put(law)

                    # Save every 20 instead of 10 to reduce lock risk
                    if len(all_rows) % 20 == 0:
                        save_excel()
    except Exception as e:
        print(f"Error processing category {category_name}: {e}")


# ----------------- MAIN -----------------
def main():
    global stop_signal

    print("\nStarting Scraping + Downloading (Safe Mode)\n")

    try:
        r = session.get(START_URL, timeout=15)
        category_links = re.findall(
            r'<a href="(classificazioni\.php\?arc=vig[^"]+)"[^>]*>(.*?)</a>',
            r.text
        )
    except Exception as e:
        print(f"Critical Error loading start URL: {e}")
        return

    WORKERS = 4 # Reduced slightly for stability
    threads = []

    for i in range(WORKERS):
        t = threading.Thread(target=print_pdf_worker, args=(i,))
        t.start()
        threads.append(t)

    progress = tqdm(total=len(category_links), desc="Categories")

    for link, name in category_links:
        progress.update(1)
        full_url = BASE_URL + link
        process_category(full_url, name.strip())

    progress.close()

    print("\nWaiting for remaining downloads...\n")
    download_queue.join()

    stop_signal = True

    for t in threads:
        t.join()

    print("\nSaving Final Excel...")
    # Retry loop for final save
    for i in range(3):
        try:
            save_excel()
            print(f"Excel Saved Successfully: {EXCEL_PATH}")
            break
        except:
            print("Excel locked. Retrying in 2 seconds...")
            time.sleep(2)

    print("\n----- Completed -----")
    print(f"Downloaded: {downloaded}")
    print(f"Skipped: {skipped}")
    print(f"Failed: {failed}")


if __name__ == "__main__":
    main()