import os
import time
import base64
import re
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import requests

# --- CONFIGURATION ---
BASE_URL = "https://leggi.alumbria.it/"
START_URL = "https://leggi.alumbria.it/leggi_02.php"
OUTPUT_FOLDER = os.path.join(os.getcwd(), "Umbria_Laws_Scrape")
EXCEL_FILENAME = "Umbria_Laws_Index.xlsx"
MAX_WORKERS = 3
TIMEOUT_SECONDS = 20  # ⏳ Max wa9t l kol page (20 seconds)

# Italian Month Mapping
ITALIAN_MONTHS = {
    'gennaio': '01', 'febbraio': '02', 'marzo': '03', 'aprile': '04',
    'maggio': '05', 'giugno': '06', 'luglio': '07', 'agosto': '08',
    'settembre': '09', 'ottobre': '10', 'novembre': '11', 'dicembre': '12'
}

# Suppress WDM logs
os.environ['WDM_LOG_LEVEL'] = '0'

if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

stats = {"Downloaded": 0, "Skipped": 0, "Failed": 0}

def setup_driver():
    """Initializes Headless Chrome."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("--window-size=1920,1080")
    
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

def convert_date_for_filename(date_str):
    """Converts '9 dicembre 2025' -> '2025-12-09'."""
    try:
        date_str = re.sub(r'\s+', ' ', date_str.strip().lower())
        for it_month, num_month in ITALIAN_MONTHS.items():
            if it_month in date_str:
                temp_date = date_str.replace(it_month, num_month)
                dt_obj = datetime.strptime(temp_date, "%d %m %Y")
                return dt_obj.strftime("%Y-%m-%d")
        return "0000-00-00"
    except Exception:
        return "0000-00-00"

def save_as_pdf(driver, filepath):
    """Saves PDF via Chrome DevTools."""
    try:
        result = driver.execute_cdp_cmd("Page.printToPDF", {
            "printBackground": True,
            "paperWidth": 8.27, "paperHeight": 11.69,
            "marginTop": 0.4, "marginBottom": 0.4,
            "displayHeaderFooter": False
        })
        with open(filepath, "wb") as f:
            f.write(base64.b64decode(result['data']))
        return True
    except Exception:
        return False

def extract_metadata(soup, url_text):
    full_text = soup.get_text(" ", strip=True)
    
    # 1. LAW TITLE
    law_title = "No Title Found"
    tds = soup.find_all('td')
    for td in tds:
        txt = td.get_text(strip=True)
        if len(txt) > 20 and not re.match(r'^\d', txt): 
            law_title = txt
            break 
    
    if law_title == "No Title Found":
        header = soup.find(['h1', 'h2', 'h3'])
        if header:
            law_title = header.get_text(strip=True)

    # 2. DATE
    date_match = re.search(r"(\d{1,2}\s+(?:gennaio|febbraio|marzo|aprile|maggio|giugno|luglio|agosto|settembre|ottobre|novembre|dicembre)\s+\d{4})", full_text, re.IGNORECASE)
    clean_date_text = date_match.group(1) if date_match else "Unknown Date"

    # 3. NUMBER
    num_match = re.search(r"n\.\s*(\d+)", full_text, re.IGNORECASE)
    if not num_match:
        num_match = re.search(r"-(\d+)\.", url_text)
    law_num = num_match.group(1) if num_match else "Unknown"

    return law_title, law_num, clean_date_text

def process_single_law(link_data):
    driver = None
    result_data = None
    status = "Failed"

    try:
        driver = setup_driver()
        # Timeout settings
        driver.set_page_load_timeout(TIMEOUT_SECONDS)
        driver.set_script_timeout(TIMEOUT_SECONDS)

        driver.get(link_data['url'])
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        title, num, date_text = extract_metadata(soup, link_data['text'])

        # Filename logic
        filename_date = convert_date_for_filename(date_text)
        region = "Umbria"
        filename = f"{region}_{num}_{filename_date}.pdf"
        filepath = os.path.join(OUTPUT_FOLDER, filename)

        if not os.path.exists(filepath):
            if save_as_pdf(driver, filepath):
                status = "Downloaded"
            else:
                status = "Failed"
        else:
            status = "Skipped"

        result_data = {
            "Region": region,
            "Law Title": title,
            "Law Number": num,
            "Date": date_text,
            "Filename": filename,
            "URL": link_data['url']
        }

    except TimeoutException:
        status = "Failed"
    except Exception:
        status = "Failed"
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass
    
    return result_data, status

def get_all_links_sorted():
    print("🔍 Scanning main page for links...")
    try:
        resp = requests.get(START_URL, timeout=15)
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        links = []
        for a in soup.find_all('a', href=True):
            if "mostra_atto.php" in a['href']:
                full_url = BASE_URL + a['href'] if not a['href'].startswith('http') else a['href']
                
                match = re.search(r"lr(\d{4})-(\d+)", full_url)
                if match:
                    year = int(match.group(1))
                    num = int(match.group(2))
                    sort_key = (year, num)
                else:
                    sort_key = (9999, 9999)

                links.append({
                    "url": full_url, 
                    "text": a.text.strip(),
                    "sort_key": sort_key
                })
        
        seen = set()
        unique_links = []
        for l in links:
            if l['url'] not in seen:
                unique_links.append(l)
                seen.add(l['url'])
        
        print("📅 Sorting links by date (Year/Number)...")
        unique_links.sort(key=lambda x: x['sort_key'])
        
        return unique_links
    except Exception as e:
        print(f"Error fetching list: {e}")
        return []

def main():
    links = get_all_links_sorted()
    total_links = len(links)
    
    if total_links == 0:
        print("❌ No links found.")
        return

    print(f"📋 Found {total_links} laws. Starting sorted download...")
    all_data = []

    # Format dyal l-barr bash yban n9i (Clean Bar)
    bar_format = "{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]"

    with tqdm(total=total_links, desc="Processing", position=0, bar_format=bar_format) as pbar:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for link in links:
                futures.append(executor.submit(process_single_law, link))
            
            for future in as_completed(futures):
                try:
                    data, status = future.result()
                    
                    stats[status] += 1
                    pbar.set_postfix(stats)
                    pbar.update(1)

                    if data:
                        all_data.append(data)

                    if len(all_data) % 5 == 0:
                        df = pd.DataFrame(all_data)
                        df.to_excel(os.path.join(OUTPUT_FOLDER, EXCEL_FILENAME), index=False)
                except Exception:
                    pbar.update(1)

    if all_data:
        df = pd.DataFrame(all_data)
        df['SortDate'] = pd.to_datetime(df['Date'], errors='coerce', format='%d %B %Y') 
        df.drop(columns=['SortDate'], errors='ignore', inplace=True)
        df.to_excel(os.path.join(OUTPUT_FOLDER, EXCEL_FILENAME), index=False)
        print(f"\n✅ Success! Data saved to: {EXCEL_FILENAME}")

if __name__ == "__main__":
    main()