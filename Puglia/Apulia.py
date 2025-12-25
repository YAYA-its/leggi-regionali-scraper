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
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# --- CONFIGURATION ---
START_URL = "https://bussolanormativa.consiglio.puglia.it/public/Leges/RicercaSemplice.aspx"
OUTPUT_FOLDER = os.path.join(os.getcwd(), "Puglia_Laws_Final")
EXCEL_FILENAME = "Puglia_Laws_Index.xlsx"
MAX_WORKERS = 3       # Parallel downloads (Safe number for headless)
MAX_PAGES = 107       # Total pages to process

# --- HEADERS / SESSION LOGIC ---
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

# Global accumulator
ALL_DATA = []

def setup_driver(headless=True):
    """
    Initializes Chrome with Session + Headers logic.
    Default is now headless=True.
    """
    chrome_options = Options()
    
    # 1. ADD HEADERS (User-Agent)
    chrome_options.add_argument(f"user-agent={USER_AGENT}")
    
    # 2. SESSION STABILITY SETTINGS
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled") 
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    # 3. HEADLESS MODE
    if headless:
        chrome_options.add_argument("--headless")
    
    chrome_options.add_argument("--start-maximized") 
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--log-level=3")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    # 4. MASK BOT VIA CDP
    driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": USER_AGENT})
    
    return driver

def clean_filename(text):
    text = re.sub(r'[\\/*?:"<>|]', "", text)
    text = text.replace("\n", " ").strip()
    return text[:50]

def save_as_pdf(driver, filepath):
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

def extract_metadata(driver):
    try:
        try:
            raw_date = driver.find_element(By.ID, "ContentPlaceHolder1_lblData").text.strip()
        except:
            raw_date = "01/01/1900"
        try:
            raw_num = driver.find_element(By.ID, "ContentPlaceHolder1_lblNumero").text.strip()
        except:
            raw_num = "Unknown"
        try:
            titles = driver.find_elements(By.XPATH, "//p[@align='center']")
            raw_title = "Title Not Found"
            for t in titles:
                if len(t.text.strip()) > 15: 
                    raw_title = t.text.strip()
                    break
        except:
            raw_title = "Title Not Found"
        return raw_title, raw_num, raw_date
    except Exception:
        return "Error", "0", "01/01/1900"

def process_law_worker(url):
    """Worker: Opens URL with Headers -> Scrapes -> Downloads PDF."""
    driver = None
    res = None
    status = "Failed"
    
    try:
        # Worker always runs headless
        driver = setup_driver(headless=True) 
        driver.get(url)
        
        # Wait for data
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "ContentPlaceHolder1_lblData")))
        
        title, num, raw_date = extract_metadata(driver)

        # Date Clean
        try:
            dt = datetime.strptime(raw_date, "%d/%m/%Y")
            file_date = dt.strftime("%Y-%m-%d")
        except:
            file_date = "0000-00-00"

        filename = f"Puglia_{clean_filename(num)}_{file_date}.pdf"
        filepath = os.path.join(OUTPUT_FOLDER, filename)

        if not os.path.exists(filepath):
            if save_as_pdf(driver, filepath):
                status = "Downloaded"
            else:
                status = "Failed"
        else:
            status = "Skipped"

        res = {
            "Region": "Puglia",
            "Law Title": title,
            "Law Number": num,
            "Date": raw_date,
            "Filename": filename,
            "URL": url
        }

    except Exception:
        status = "Failed"
    finally:
        if driver:
            driver.quit()
    
    return res, status

def save_excel_batch():
    if not ALL_DATA:
        return
    df = pd.DataFrame(ALL_DATA)
    # Sort
    df['Sort'] = pd.to_datetime(df['Date'], format="%d/%m/%Y", errors='coerce')
    df.sort_values('Sort', inplace=True)
    df.drop(columns=['Sort'], inplace=True)
    
    excel_path = os.path.join(OUTPUT_FOLDER, EXCEL_FILENAME)
    df.to_excel(excel_path, index=False)

def run_main_process():
    # Main driver is now HEADLESS
    driver = setup_driver(headless=True) 
    wait = WebDriverWait(driver, 15)
    
    try:
        print(f"🔍 Accessing Search Page (Headless Mode)...")
        driver.get(START_URL)

        # --- STEP 1: SELECT FILTER ---
        print("👆 Selecting 'Leggi Regionali'...")
        try:
            label_element = wait.until(EC.element_to_be_clickable((By.XPATH, "//label[@for='Leggi']")))
            label_element.click()
        except:
            checkbox = driver.find_element(By.ID, "Leggi")
            driver.execute_script("arguments[0].click();", checkbox)
        
        time.sleep(1)

        # --- STEP 2: SEARCH ---
        print("👆 Clicking Search...")
        search_btn = driver.find_element(By.ID, "ContentPlaceHolder1_btnInvia")
        driver.execute_script("arguments[0].click();", search_btn)
        
        print("⏳ Waiting for results...")
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        time.sleep(2) 

        # --- STEP 3: BATCH PROCESSING ---
        print(f"🚀 Starting Batch Processing (1 to {MAX_PAGES})...")
        
        for page_num in range(1, MAX_PAGES + 1):
            print(f"\n📄 --- Processing Page {page_num}/{MAX_PAGES} ---")

            # A. Navigate
            if page_num > 1:
                try:
                    try:
                        page_link = driver.find_element(By.LINK_TEXT, str(page_num))
                    except:
                        print(f"   ⚠️ Link '{page_num}' hidden. Clicking '...'")
                        next_dots = driver.find_elements(By.XPATH, "//a[contains(text(), '...')]")
                        if next_dots:
                            driver.execute_script("arguments[0].click();", next_dots[-1])
                            time.sleep(3)
                            page_link = driver.find_element(By.LINK_TEXT, str(page_num))
                        else:
                            raise Exception("Pagination link not found.")

                    first_row_before = driver.find_element(By.XPATH, "//tr[2]").text 
                    driver.execute_script("arguments[0].click();", page_link)
                    
                    WebDriverWait(driver, 10).until(
                        lambda d: d.find_element(By.XPATH, "//tr[2]").text != first_row_before
                    )
                    time.sleep(1)
                
                except Exception as e:
                    print(f"⚠️ Failed to navigate to Page {page_num}: {e}")
                    continue

            # B. Get Links
            current_elements = driver.find_elements(By.XPATH, "//a[contains(@href, 'LeggeNavscroll.aspx')]")
            batch_links = []
            for el in current_elements:
                href = el.get_attribute('href')
                if href and href not in batch_links:
                    batch_links.append(href)
            
            print(f"   🔗 Found {len(batch_links)} laws. Starting parallel download...")

            # C. Download
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(process_law_worker, url) for url in batch_links]
                
                # Progress bar for current batch
                for future in tqdm(as_completed(futures), total=len(batch_links), desc=f"   Batch {page_num}", leave=False):
                    data, status = future.result()
                    if data:
                        ALL_DATA.append(data)

            # D. Save to Excel
            save_excel_batch()

    except Exception as e:
        print(f"❌ Critical Error: {e}")
    finally:
        driver.quit()
        print("\n✅ Process Completed.")

if __name__ == "__main__":
    run_main_process()