import os
import re
import time
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

# Selenium Imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# --- CONFIGURATION ---
BASE_URL = "https://www.consiglioveneto.it/leggi-regionali"
REGION_NAME = "Veneto"
OUTPUT_DIR = os.path.abspath("Veneto_Laws_Final")
EXCEL_FILE = "Veneto_Laws_Data.xlsx"

# Regex for metadata
METADATA_PATTERN = r"(?i)Legge\s+regionale\s+(?P<date>.*?)(?:,|\s+)\s+n\.\s+(?P<number>\d+)"

# Create output directory
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

scraped_data = []

def clean_filename(text):
    return re.sub(r'[\\/*?:"<>|]', "", text).strip()

def format_italian_date(date_str):
    italian_months = {
        'gennaio': '01', 'febbraio': '02', 'marzo': '03', 'aprile': '04',
        'maggio': '05', 'giugno': '06', 'luglio': '07', 'agosto': '08',
        'settembre': '09', 'ottobre': '10', 'novembre': '11', 'dicembre': '12'
    }
    try:
        clean_str = re.sub(r'\s+', ' ', date_str.strip().lower())
        parts = clean_str.split(' ')
        if len(parts) >= 3:
            day = parts[0].zfill(2)
            month_name = parts[1]
            year = parts[2]
            if month_name in italian_months:
                return f"{year}-{italian_months[month_name]}-{day}"
    except Exception:
        pass
    return clean_filename(date_str).replace(" ", "-")

def save_excel():
    if scraped_data:
        df = pd.DataFrame(scraped_data)
        cols = ["Region", "Law Number", "Date", "Law Title", "Filename"]
        final_cols = [c for c in cols if c in df.columns]
        df[final_cols].to_excel(EXCEL_FILE, index=False)

def wait_and_rename(target_filename, timeout=30):
    end_time = time.time() + timeout
    initial_files = set(os.listdir(OUTPUT_DIR))
    
    while time.time() < end_time:
        current_files = set(os.listdir(OUTPUT_DIR))
        new_files = current_files - initial_files
        
        for f in new_files:
            if f.endswith(".crdownload") or f.endswith(".tmp"):
                continue
            try:
                full_path = os.path.join(OUTPUT_DIR, f)
                final_path = os.path.join(OUTPUT_DIR, target_filename)
                
                time.sleep(1) 
                
                if os.path.exists(final_path):
                    try:
                        os.remove(final_path)
                    except:
                        pass
                    
                os.rename(full_path, final_path)
                return True
            except OSError:
                time.sleep(1)
                continue
        time.sleep(1)
    return False

def setup_driver():
    options = webdriver.ChromeOptions()
    # Use "new" headless for better download support
    options.add_argument("--headless=new") 
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--log-level=3")
    
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    options.add_argument(f'user-agent={user_agent}')
    
    prefs = {
        "download.default_directory": OUTPUT_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
        "profile.default_content_settings.popups": 0,
        "profile.default_content_setting_values.automatic_downloads": 1 
    }
    options.add_experimental_option("prefs", prefs)
    
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

def main():
    print("--- Starting Veneto Scraper (Year Fix & Smart Skip) ---")
    driver = setup_driver()
    
    total_downloaded = 0
    total_skipped = 0

    try:
        driver.get(BASE_URL)
        wait = WebDriverWait(driver, 15)
        
        print("Detecting available years...")
        try:
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "filters")))
            wait.until(EC.presence_of_all_elements_located((By.XPATH, "//button[contains(@onclick, 'setFilterAnno')]")))
        except Exception:
            print("  [Warning] Timeout waiting for year buttons.")

        buttons = driver.find_elements(By.XPATH, "//button[contains(@onclick, 'setFilterAnno')]")
        years = []
        for btn in buttons:
            try:
                txt = btn.get_attribute("onclick")
                match = re.search(r'setFilterAnno\(.*?,(\d{4})\)', txt)
                if match:
                    years.append(match.group(1))
            except:
                continue
        
        years = sorted(list(set(years)), reverse=True)
        print(f"Years found: {years}")

        if not years:
            print("  [Error] No years found.")
            return

        for year in years:
            print(f"\n==== Processing Year: {year} ====")
            
            # 👇 CORE FIX: Click the button explicitly instead of relying on URL
            try:
                # Find button specifically for this year
                xpath = f"//button[contains(@onclick, 'setFilterAnno') and contains(@onclick, '{year}')]"
                year_btn = driver.find_element(By.XPATH, xpath)
                driver.execute_script("arguments[0].click();", year_btn)
                print(f"  [Action] Clicked button for {year}")
                time.sleep(3) # Wait for AJAX reload
            except Exception as e:
                print(f"  [Warning] Could not click button for {year}. Trying URL fallback.")
                driver.get(f"{BASE_URL}?annoSelezionato={year}")
                time.sleep(3)

            # Prevent Infinite Loops
            visited_urls = set()

            while True:
                current_url = driver.current_url
                if current_url in visited_urls:
                    print(f"  ⚠️ Loop detected (Page visited twice). Next year.")
                    break
                visited_urls.add(current_url)

                # Find laws on page
                law_links = driver.find_elements(By.XPATH, "//a[contains(@href, 'dettaglio-legge')]")
                page_urls = list(set([l.get_attribute('href') for l in law_links]))
                
                print(f"  Found {len(page_urls)} laws on this page.")

                if len(page_urls) == 0:
                    print("  [Info] No laws found on this page.")
                    break

                for law_url in page_urls:
                    try:
                        driver.get(law_url)
                        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "title")))
                        
                        soup = BeautifulSoup(driver.page_source, 'html.parser')
                        h2 = soup.find('h2', class_='title')
                        h2_text = h2.get_text(strip=True) if h2 else ""
                        p_title = soup.find('p', class_='font-18')
                        law_title = p_title.get_text(strip=True) if p_title else "No Title"

                        law_date = "Unknown"
                        law_num = "Unknown"
                        match = re.search(METADATA_PATTERN, h2_text)
                        if match:
                            law_date = match.group("date").strip()
                            law_num = match.group("number").strip()

                        formatted_date = format_italian_date(law_date)
                        filename = f"{REGION_NAME}_{law_num}_{formatted_date}.pdf"
                        file_path = os.path.join(OUTPUT_DIR, filename)

                        record = {
                            "Region": REGION_NAME,
                            "Law Title": law_title,
                            "Law Number": law_num,
                            "Date": law_date,
                            "Filename": filename
                        }

                        # 👇 SKIP CHECK (Before any download action)
                        if os.path.exists(file_path):
                            print(f"    [SKIP] Already exists: {filename}")
                            total_skipped += 1
                        else:
                            # Download
                            try:
                                download_btn = driver.find_element(By.XPATH, "//a[contains(., 'Pdf testo')]")
                                driver.execute_script("arguments[0].click();", download_btn)
                                
                                if wait_and_rename(filename):
                                    total_downloaded += 1
                                    print(f"    [OK] Downloaded: {filename}")
                                else:
                                    print(f"    [ERR] Download Timeout")
                                    record['Filename'] = "Failed Download"
                            except Exception:
                                print(f"    [SKIP] No PDF Button found")
                                record['Filename'] = "No PDF"

                        scraped_data.append(record)
                        if len(scraped_data) % 10 == 0:
                            save_excel()

                    except Exception as e:
                        print(f"    [ERR] Error processing law: {e}")

                # Pagination Logic: Go back to list
                driver.get(current_url)
                time.sleep(2)
                
                try:
                    # Look for "Successiva" (Next)
                    next_btns = driver.find_elements(By.XPATH, "//a[contains(text(), 'Successiva')]")
                    if next_btns:
                        next_url = next_btns[0].get_attribute("href")
                        
                        if next_url in visited_urls or next_url == current_url:
                            print("  [End] Next page is same. Stopping year.")
                            break
                            
                        if "javascript" not in next_url:
                            print("  Moving to next page...")
                            driver.get(next_url)
                            time.sleep(2)
                            continue
                    
                    print("  [End] No 'Successiva' button found.")
                    break
                except:
                    break
            
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        driver.quit()
        save_excel()
        print(f"\nDone. Downloaded: {total_downloaded}, Skipped: {total_skipped}.")

if __name__ == "__main__":
    main()