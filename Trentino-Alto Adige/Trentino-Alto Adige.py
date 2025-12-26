import os
import re
import time
import requests
import pandas as pd
from urllib.parse import urljoin
from tqdm import tqdm

# --- Selenium Imports ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException

# --- Configuration ---
START_URL = "https://www.regione.taa.it/Documenti/Atti-normativi"
BASE_URL = "https://www.regione.taa.it"
DOWNLOAD_DIR = "pdf_downloads"
EXCEL_FILE = "laws_metadata.xlsx"

# Month Map
MONTH_MAP = {
    'gennaio': '01', 'febbraio': '02', 'marzo': '03', 'aprile': '04',
    'maggio': '05', 'giugno': '06', 'luglio': '07', 'agosto': '08',
    'settembre': '09', 'ottobre': '10', 'novembre': '11', 'dicembre': '12',
    'january': '01', 'february': '02', 'march': '03', 'april': '04',
    'may': '05', 'june': '06', 'july': '07', 'august': '08',
    'september': '09', 'october': '10', 'november': '11', 'december': '12'
}

def setup_driver():
    options = webdriver.ChromeOptions()
    # --- HEADLESS MODE ---
    options.add_argument("--headless=new") 
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")
    
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

def clean_date_string(text):
    if not text: return None, None
    text = text.replace("Data", "").replace("\n", " ").strip()
    
    match_slash = re.search(r'(\d{1,2})[./-](\d{1,2})[./-](\d{4})', text)
    if match_slash:
        d, m, y = match_slash.groups()
        return f"{d.zfill(2)}/{m.zfill(2)}/{y}", f"{y}-{m.zfill(2)}-{d.zfill(2)}"

    match_txt = re.search(r'(\d{1,2})\s+([a-zA-Z]+)\s+(\d{4})', text)
    if not match_txt:
        match_txt = re.search(r'([a-zA-Z]+)\s+(\d{1,2}),?\s+(\d{4})', text)
        if match_txt: m_txt, d, y = match_txt.groups()
        else: return None, None
    else:
        d, m_txt, y = match_txt.groups()

    m_num = MONTH_MAP.get(m_txt.lower())
    if m_num: return f"{d.zfill(2)}/{m_num}/{y}", f"{y}-{m_num}-{d.zfill(2)}"
    return None, None

def determine_pdf_type(url_or_name):
    """
    Simplified: Accepts any PDF link. 
    Returns 'IT' if Italian is detected, 'DE' if German, else 'UNK'.
    """
    text = url_or_name.lower()
    if "_it" in text or "-it." in text: return "IT"
    if "_st" in text or "-st." in text or "_de" in text: return "DE"
    return "DOC" # Default type

def main():
    if not os.path.exists(DOWNLOAD_DIR): os.makedirs(DOWNLOAD_DIR)

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive"
    })

    print("--- Launching Headless Scraper (Fixed PDF Logic) ---")
    driver = setup_driver()
    wait = WebDriverWait(driver, 15)
    all_data = []
    
    # PAGE COUNTER
    page_num = 1

    try:
        driver.get(START_URL)
        time.sleep(2)

        try:
            btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'Accetta') or contains(text(),'Acconsento')]")))
            btn.click()
            time.sleep(1)
        except: pass

        print("Applying Filter...")
        try:
            filter_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a[data-tag_id='13618']")))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", filter_btn)
            time.sleep(1)
            filter_btn.click()
            time.sleep(5)
        except Exception as e:
            print(f"Filter Error: {e}"); return

        while True:
            # PRINT PAGE NUMBER
            print(f"\n--- Processing Page {page_num} ---")
            
            law_urls = []
            elements = driver.find_elements(By.CSS_SELECTOR, "a[href*='/content/view/full/']")
            for el in elements: law_urls.append(el.get_attribute('href'))
            law_urls = list(set(law_urls))

            print(f"Found {len(law_urls)} laws...")

            for url in tqdm(law_urls, desc=f"Page {page_num}"):
                try:
                    driver.get(url)
                    
                    # A. TITLE
                    title = "Unknown"
                    try:
                        h1_elem = driver.find_element(By.TAG_NAME, "h1")
                        title_text = driver.execute_script("return arguments[0].nextSibling.textContent;", h1_elem).strip()
                        if len(title_text) > 5: title = title_text
                        else: title = h1_elem.text.strip()
                    except:
                        try: title = driver.find_element(By.CSS_SELECTOR, "font[dir='auto']").text.strip()
                        except: pass

                    # B. DATE
                    excel_date = "Unknown"
                    file_date_iso = "0000-00-00"
                    date_candidates = []
                    
                    try: strongs = driver.find_elements(By.CSS_SELECTOR, "strong.text-nowrap"); date_candidates.extend([s.text for s in strongs])
                    except: pass
                    try: 
                        d_el = driver.find_element(By.XPATH, "//div[contains(@class, 'col-md-3')][contains(., 'Data')]")
                        date_candidates.append(d_el.text)
                    except: pass

                    for txt in date_candidates:
                        ed, fd = clean_date_string(txt)
                        if ed: excel_date = ed; file_date_iso = fd; break
                    
                    if file_date_iso == "0000-00-00":
                        y_match = re.search(r'20\d{2}|19\d{2}', title)
                        if y_match: y = y_match.group(0); excel_date = f"01/01/{y}"; file_date_iso = f"{y}-01-01"

                    # C. LAW NUMBER
                    try:
                        h1_text = driver.find_element(By.TAG_NAME, "h1").text
                        n_match = re.search(r'n\.?\s*(\d+)', h1_text, re.IGNORECASE)
                        law_num = n_match.group(1) if n_match else "0"
                    except: law_num = "0"

                    # D. PROCESS PDFs (Fixed Selector)
                    # We look for ANY download link inside "card-teaser" or "download-list"
                    pdf_elements = driver.find_elements(By.CSS_SELECTOR, "div.card-teaser a, div.download-list a")
                    
                    if not pdf_elements:
                         # Fallback: Look for any link ending in .pdf
                         pdf_elements = driver.find_elements(By.CSS_SELECTOR, "a[href$='.pdf']")

                    for pdf_el in pdf_elements:
                        pdf_href = pdf_el.get_attribute('href')
                        if not pdf_href: continue

                        # Determine type or default
                        suffix = determine_pdf_type(pdf_href)
                        
                        filename = f"TrentinoAA_{law_num}_{file_date_iso}_{suffix}.pdf"
                        save_path = os.path.join(DOWNLOAD_DIR, filename)
                        
                        # 👇 SKIP CHECK HERE
                        status = "Skipped"
                        if not os.path.exists(save_path):
                            try:
                                # Use requests session to download (more reliable than selenium here)
                                r = session.get(pdf_href, stream=True, timeout=30)
                                if r.status_code == 200 and 'application/pdf' in r.headers.get('Content-Type', ''):
                                    with open(save_path, 'wb') as f:
                                        for chunk in r.iter_content(chunk_size=8192): f.write(chunk)
                                    status = "Downloaded"
                                else:
                                    status = f"Failed ({r.status_code})"
                            except: status = "Failed"
                        
                        all_data.append({
                            "Page": page_num,
                            "Region": "Trentino-Alto Adige",
                            "Law Title": title,
                            "Law Number": law_num,
                            "Date": excel_date,
                            "Type": suffix,
                            "Filename": filename,
                            "Status": status,
                            "URL": url
                        })

                except Exception: pass
            
            pd.DataFrame(all_data).to_excel(EXCEL_FILE, index=False)

            # Pagination
            print("Returning to list...")
            driver.get(START_URL)
            time.sleep(2)
            try:
                filter_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a[data-tag_id='13618']")))
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", filter_btn)
                time.sleep(1)
                filter_btn.click()
                time.sleep(4)
                
                # Navigate to next page
                # We need to click "Successiva" X times to get back to where we were
                # BUT this site handles pagination weirdly.
                # A safer way is to find the page number link.
                
                # Try finding the next page number directly
                try:
                    next_page_link = driver.find_element(By.XPATH, f"//a[contains(@class, 'page-link') and text()='{page_num + 1}']")
                    driver.execute_script("arguments[0].click();", next_page_link)
                    time.sleep(5)
                    page_num += 1
                except:
                    # Try "Successiva" button as fallback
                    next_btn = None
                    selectors = ["//a[@title='Successiva']", "//a[contains(@class, 'next')]", "//li[contains(@class,'next')]//a"]
                    for xpath in selectors:
                        try:
                            btn = driver.find_element(By.XPATH, xpath)
                            if btn.is_displayed(): next_btn = btn; break
                        except: continue

                    if next_btn:
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_btn)
                        driver.execute_script("arguments[0].click();", next_btn)
                        time.sleep(5)
                        page_num += 1
                    else:
                        print("No more pages found.")
                        break
            except: break

    except Exception as e: print(f"Error: {e}")
    finally:
        driver.quit()
        if all_data: pd.DataFrame(all_data).to_excel(EXCEL_FILE, index=False)
        print("Done.")

if __name__ == "__main__":
    main()