import os
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import time

# --- CONFIGURATION ---
BASE_URL = "https://www.consiglio.regione.lazio.it"
SEARCH_URL = "https://www.consiglio.regione.lazio.it/index.php"
DOWNLOAD_DIR = "downloads"
EXCEL_FILE = "scraped_laws.xlsx"
REGION = "Lazio"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

stats = {"Downloaded": 0, "Skipped": 0, "Failed": 0}

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/pdf,text/html,application/xhtml+xml"
})

def clean_date(text):
    match = re.search(r'(\d{1,2}\s+[a-zA-Z]+\s+\d{4})', text)
    return match.group(1) if match else "N/A"

def format_date_filename(date_str):
    months = {
        'gennaio': '01', 'febbraio': '02', 'marzo': '03', 'aprile': '04',
        'maggio': '05', 'giugno': '06', 'luglio': '07', 'agosto': '08',
        'settembre': '09', 'ottobre': '10', 'novembre': '11', 'dicembre': '12'
    }
    parts = date_str.lower().split()
    if len(parts) == 3:
        return f"{parts[2]}-{months.get(parts[1], '00')}-{parts[0].zfill(2)}"
    return "unknown_date"

def download_file(detail_url, filename, pbar_download):
    global stats
    try:
        # CONVERT detail URL to PDF URL
        # Change 'vw=leggiregionalidettaglio' to 'vw=pdf'
        pdf_url = detail_url.replace("vw=leggiregionalidettaglio", "vw=pdf")
        
        path = os.path.join(DOWNLOAD_DIR, filename)
        if os.path.exists(path):
            stats["Skipped"] += 1
        else:
            # Requesting the PDF stream
            response = session.get(pdf_url, stream=True, timeout=30)
            
            # Check if the response is actually a PDF
            if "application/pdf" in response.headers.get('Content-Type', '').lower():
                with open(path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=1024 * 8):
                        if chunk:
                            f.write(chunk)
                stats["Downloaded"] += 1
            else:
                # If it's not a PDF, the server might need the 'id' specifically
                stats["Failed"] += 1
    except:
        stats["Failed"] += 1
    finally:
        pbar_download.update(1)
        pbar_download.set_postfix(stats)

def run_scraper():
    results = []
    params = {"sv": "vigente", "vw": "leggiregionali", "invia": " Cerca "}

    print("Fetching total page count...")
    r = session.get(SEARCH_URL, params=params)
    soup = BeautifulSoup(r.text, 'html.parser')
    
    # Identify pagination
    page_links = soup.select('ul.pagination li a')
    max_page = 1
    for link in page_links:
        m = re.search(r'page=(\d+)', link.get('href', ''))
        if m: max_page = max(max_page, int(m.group(1)))

    p_bar = tqdm(total=max_page, desc="Pages    ", position=0)
    s_bar = tqdm(desc="Scraping ", position=1, unit="law")
    d_bar = tqdm(desc="Downloads", position=2, unit="pdf")

    with ThreadPoolExecutor(max_workers=5) as executor:
        for page_num in range(1, max_page + 1):
            p_params = params.copy()
            p_params["page"] = page_num
            
            resp = session.get(SEARCH_URL, params=p_params)
            page_soup = BeautifulSoup(resp.text, 'html.parser')
            
            # The actual result rows
            items = page_soup.find_all('div', class_='riga-risultato')
            
            s_bar.total = (s_bar.total or 0) + len(items)
            d_bar.total = (d_bar.total or 0) + len(items)

            for item in items:
                try:
                    link_tag = item.find('a', class_='titolo-legge')
                    title = link_tag.text.strip()
                    detail_path = link_tag['href']
                    detail_url = BASE_URL + detail_path if detail_path.startswith('/') else BASE_URL + '/' + detail_path
                    
                    info_text = item.text.strip()
                    date_val = clean_date(info_text)
                    num_match = re.search(r'n\.\s*(\d+)', info_text)
                    law_num = num_match.group(1) if num_match else "0"
                    
                    filename = f"{REGION}_{law_num}_{format_date_filename(date_val)}.pdf"
                    
                    results.append({
                        "Region": REGION, "Law Title": title, 
                        "Law Number": law_num, "Date": date_val, "Filename": filename
                    })
                    
                    # Submit download task
                    executor.submit(download_file, detail_url, filename, d_bar)
                except Exception as e:
                    stats["Failed"] += 1
                
                s_bar.update(1)
            
            # Save Excel after every page
            pd.DataFrame(results).to_excel(EXCEL_FILE, index=False)
            p_bar.update(1)

    print(f"\nFinished! Check the '{DOWNLOAD_DIR}' folder.")

if __name__ == "__main__":
    run_scraper()