import os
import re
import time
import base64
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
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import requests

# --- CONFIGURATION ---
INDEX_URL = "http://arianna.consiglioregionale.piemonte.it/iterlegfo/"
START_URL = "http://arianna.consiglioregionale.piemonte.it/iterlegfo/indiceCronoLeggi.do"
OUTPUT_FOLDER = os.path.join(os.getcwd(), "Piemonte_Laws_Final")
EXCEL_FILENAME = "Piemonte_Laws_Index.xlsx"

# ⚠️ SETTINGS ⚠️
VISIBLE_MODE = False   # Set to False for Headless
MAX_WORKERS = 3        # Reduced slightly to prevent server blocking your IP
MAX_RETRIES = 3 

# Firefox User Agent
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0"

ITALIAN_MONTHS = {
    'gennaio': '01', 'febbraio': '02', 'marzo': '03', 'aprile': '04',
    'maggio': '05', 'giugno': '06', 'luglio': '07', 'agosto': '08',
    'settembre': '09', 'ottobre': '10', 'novembre': '11', 'dicembre': '12'
}

if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

ALL_DATA = []

def setup_driver():
    chrome_options = Options()
    
    # --- CRITICAL FIX FOR HEADLESS PDF SAVING ---
    if not VISIBLE_MODE:
        # Use "new" headless mode which supports full PDF printing
        chrome_options.add_argument("--headless=new") 
    else:
        chrome_options.add_argument("--start-maximized")

    # Force a standard window size so elements don't overlap in headless
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Hide automation flags
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument(f"user-agent={USER_AGENT}")
    
    # Stability
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--log-level=3")
    
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

def clean_filename(text):
    text = re.sub(r'[\\/*?:"<>|]', "", text)
    text = text.replace("\n", " ").strip()
    return text[:50]

def parse_italian_date(date_str):
    try:
        date_str = date_str.lower().strip()
        for it_month, num_month in ITALIAN_MONTHS.items():
            if it_month in date_str:
                date_str = date_str.replace(it_month, num_month)
                parts = date_str.split()
                if len(parts) == 3:
                    day, month, year = parts
                    day = day.zfill(2)
                    return f"{year}-{month}-{day}"
    except:
        pass
    return "0000-00-00"

def extract_metadata(soup):
    # 1. TITLE
    title_div = soup.find("div", id="titoloAtto")
    if title_div:
        for bur in title_div.find_all("div", class_="bur"):
            bur.decompose()
        law_title = title_div.get_text(" ", strip=True)
    else:
        law_title = "Title Not Found"

    # 2. NUMBER & DATE
    lead_span = soup.find('span', class_='lead')
    full_text = lead_span.get_text(" ", strip=True) if lead_span else soup.get_text(" ", strip=True)
    
    num_match = re.search(r"n\.?\s*(\d+)", full_text, re.IGNORECASE)
    law_num = num_match.group(1) if num_match else "Unknown"

    date_match = re.search(r"del\s+(\d{1,2})\s+([a-zA-Z]+)\s+(\d{4})", full_text, re.IGNORECASE)
    
    raw_date_display = "00/00/0000"
    filename_date = "0000-00-00"

    if date_match:
        day = date_match.group(1)
        month_name = date_match.group(2)
        year = date_match.group(3)
        raw_date_display = f"{day} {month_name} {year}"
        filename_date = parse_italian_date(raw_date_display)

    return law_title, law_num, raw_date_display, filename_date

def remove_popups(driver):
    """Kills popups that might block the print view"""
    try:
        # Native Alert
        try:
            WebDriverWait(driver, 0.5).until(EC.alert_is_present())
            driver.switch_to.alert.accept()
        except:
            pass

        # HTML Overlay
        driver.execute_script("""
            var allElements = document.querySelectorAll('div, p, span, h1, h2, table, section');
            for (var i = 0; i < allElements.length; i++) {
                var txt = (allElements[i].innerText || "").toLowerCase();
                if (txt.includes("security vulnerability") || 
                    txt.includes("update your browser") || 
                    txt.includes("chrome")) {
                    allElements[i].style.display = 'none';
                    if (allElements[i].parentElement) allElements[i].parentElement.style.display = 'none';
                }
            }
            document.body.style.overflow = 'auto'; 
        """)
    except:
        pass

def is_law_abrogated(soup):
    """
    ═══════════════════════════════════════════════════════════════════════════
    STRICT ABROGATION CHECK - SINGLE RULE IMPLEMENTATION
    ═══════════════════════════════════════════════════════════════════════════
    
    PURPOSE:
    Determines if a law should be skipped based on ONE strict condition.
    
    SKIP CONDITION (THE ONLY ONE):
    - Law contains the word "Abrogata" enclosed in parentheses
    - Pattern: ( Abrogata ) or (Abrogata)
    - Regex: \(\s*Abrogata\s*\)
    
    DO NOT SKIP IF:
    ❌ Text contains "è abrogata" (without parentheses)
    ❌ Text contains "Abrogazioni"
    ❌ Text contains "abrogata" (without parentheses)
    ❌ Any other variation
    
    IMPLEMENTATION DETAILS:
    1. Extracts entire page text
    2. Applies strict regex pattern matching
    3. Returns True ONLY if pattern found in parentheses
    
    PARAMETERS:
    - soup (BeautifulSoup): Parsed HTML of the law page
    
    RETURNS:
    - True: Law is abrogated (skip it)
    - False: Law is valid (process it)
    
    ═══════════════════════════════════════════════════════════════════════════
    """
    try:
        # Extract all text from the page
        page_text = soup.get_text(" ", strip=True)
        
        # ═══════════════════════════════════════════════════════════════════
        # STRICT REGEX PATTERN - MATCHES ONLY: (Abrogata) or ( Abrogata )
        # ═══════════════════════════════════════════════════════════════════
        # Pattern breakdown:
        # \(        - Literal opening parenthesis
        # \s*       - Zero or more whitespace characters
        # Abrogata  - Exact word (case-insensitive via re.IGNORECASE flag)
        # \s*       - Zero or more whitespace characters
        # \)        - Literal closing parenthesis
        # ═══════════════════════════════════════════════════════════════════
        
        abrogata_pattern = r'\(\s*Abrogata\s*\)'
        
        # Search for the pattern (case-insensitive)
        match = re.search(abrogata_pattern, page_text, re.IGNORECASE)
        
        if match:
            # Pattern found - law is abrogated
            return True
        
        # No pattern found - law is valid
        return False
        
    except Exception as e:
        # If check fails for any reason, assume law is valid
        # (Conservative approach to avoid false positives)
        return False

def process_law_worker(law_url):
    """
    ═══════════════════════════════════════════════════════════════════════════
    LAW PROCESSING WORKER WITH STRICT ABROGATION FILTERING
    ═══════════════════════════════════════════════════════════════════════════
    
    WORKFLOW:
    1. Initialize Selenium driver
    2. Load the law page
    3. Wait for content to render
    4. Remove any blocking popups
    5. Parse page content with BeautifulSoup
    6. **CHECK ABROGATION STATUS** (CRITICAL STEP)
       - If (Abrogata) pattern found → SKIP and close driver
       - If pattern NOT found → Continue processing
    7. Extract metadata (title, number, date)
    8. Generate PDF using Chrome DevTools Protocol
    9. Save PDF to disk
    10. Validate file size
    11. Return result and status
    
    ABROGATION LOGIC:
    - Uses is_law_abrogated() function
    - Returns immediately if law is abrogated
    - Status: "Skipped (Abrogated)"
    - No PDF is generated for abrogated laws
    
    ═══════════════════════════════════════════════════════════════════════════
    """
    attempt = 0
    status = "Failed"
    res = None
    
    while attempt < MAX_RETRIES:
        driver = None
        try:
            attempt += 1
            driver = setup_driver()
            driver.get(law_url)
            
            # Wait for content to actually exist
            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "titoloAtto")))
            except:
                pass 

            remove_popups(driver)
            
            # Small buffer for headless rendering
            time.sleep(1)

            # Parse page content
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # ═══════════════════════════════════════════════════════════════
            # CRITICAL ABROGATION CHECK
            # ═══════════════════════════════════════════════════════════════
            # Check if law contains (Abrogata) pattern
            # If TRUE: Skip this law and close driver immediately
            # If FALSE: Continue processing
            # ═══════════════════════════════════════════════════════════════
            
            if is_law_abrogated(soup):
                status = "Skipped (Abrogated)"
                driver.quit()
                return None, status
            
            # Law is valid - proceed with metadata extraction
            title, num, raw_date_display, filename_date = extract_metadata(soup)
            
            region = "Piemonte"
            filename = f"{region}_{clean_filename(num)}_{filename_date}.pdf"
            filepath = os.path.join(OUTPUT_FOLDER, filename)

            # SAVE PDF using CDP
            # Note: transferMode 'Return asBase64' helps avoid large object crashes
            try:
                pdf_data = driver.execute_cdp_cmd("Page.printToPDF", {
                    "landscape": False, 
                    "printBackground": True,
                    "paperWidth": 8.27, 
                    "paperHeight": 11.69,
                    "marginTop": 0.4, "marginBottom": 0.4,
                    "marginLeft": 0.4, "marginRight": 0.4,
                    "displayHeaderFooter": False,
                    "transferMode": "ReturnAsBase64" 
                })
                
                with open(filepath, "wb") as f:
                    f.write(base64.b64decode(pdf_data['data']))
                
                # Check if valid
                if os.path.getsize(filepath) > 2000: # Needs to be > 2KB to be a real page
                    status = "Downloaded"
                    res = {
                        "Region": region,
                        "Law Title": title,
                        "Law Number": num,
                        "Date": raw_date_display,
                        "Filename": filename
                    }
                    break 
                else:
                    status = "Failed (Empty)"
            except Exception as e:
                time.sleep(1)
                raise e

        except Exception as e:
            status = f"Error"
            # print(f"Err: {e}") # Uncomment to debug
        finally:
            if driver:
                driver.quit()
    
    return res, status

def get_year_list():
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    try:
        resp = session.get(START_URL, timeout=15)
        soup = BeautifulSoup(resp.content, 'html.parser')
        years = []
        for a in soup.find_all('a', href=True):
            if "elencoLeggi.do" in a['href']:
                txt = a.get_text(strip=True)
                if txt.isdigit():
                    years.append({"year": txt, "url": urljoin(INDEX_URL, a['href'])})
        return sorted(years, key=lambda x: int(x['year']), reverse=True)
    except:
        return []

def get_links_for_year(year_url):
    """
    ═══════════════════════════════════════════════════════════════════════════
    YEAR LINKS COLLECTOR - SELENIUM-BASED PAGINATION
    ═══════════════════════════════════════════════════════════════════════════
    
    PURPOSE:
    Collects all law detail links from a year's listing page(s) using Selenium
    to handle dynamic pagination.
    
    PAGINATION STRATEGY:
    1. Load initial year URL with Selenium
    2. Extract all law links from current page
    3. Look for "Next" button (» symbol) using XPATH
    4. If button exists:
       - Click it
       - Wait for new content to load
       - Repeat from step 2
    5. If button doesn't exist:
       - We're on the last page, exit loop
    
    IMPORTANT NOTE:
    This function does NOT perform any abrogation filtering.
    All links are collected regardless of status indicators in the list view.
    
    FILTERING LOCATION:
    All abrogation checking is done in process_law_worker() function.
    
    ═══════════════════════════════════════════════════════════════════════════
    """
    driver = None
    law_links = []
    max_pages = 100  # Safety limit to prevent infinite loops
    page_count = 0
    
    try:
        # Initialize Selenium driver for pagination
        driver = setup_driver()
        driver.get(year_url)
        
        # Wait for initial page load
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "dettaglio"))
            )
        except:
            pass  # Continue even if specific element not found
        
        # ═══════════════════════════════════════════════════════════════════
        # PAGINATION LOOP - Navigate through all result pages
        # ═══════════════════════════════════════════════════════════════════
        while page_count < max_pages:
            page_count += 1
            
            # Small delay to ensure page is fully loaded
            time.sleep(1)
            
            # Parse current page content
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # ───────────────────────────────────────────────────────────────
            # STEP 1: Extract all law detail links from current page
            # ───────────────────────────────────────────────────────────────
            links_found_on_page = 0
            for a in soup.find_all('a', class_='dettaglio', href=True):
                if "dettaglioLegge.do" in a['href']:
                    # Build absolute URL
                    full_link = urljoin(driver.current_url, a['href'])
                    
                    # Add only unique links
                    if full_link not in law_links:
                        law_links.append(full_link)
                        links_found_on_page += 1
            
            # Debug info (optional - uncomment for troubleshooting)
            # print(f"  Page {page_count}: Found {links_found_on_page} new links (Total: {len(law_links)})")
            
            # ═══════════════════════════════════════════════════════════════
            # CRITICAL: ZERO-LINKS EXIT CONDITION
            # ═══════════════════════════════════════════════════════════════
            # DEFENSIVE PROGRAMMING: Prevent infinite loops on empty years
            # 
            # SCENARIO: When processing years with no legislative activity
            # (e.g., future years like 2029, or years with database gaps)
            # 
            # PROBLEM WITHOUT THIS CHECK:
            # - Function finds 0 links on the page
            # - Continues to look for Next button
            # - May find false positive navigation elements
            # - Loops indefinitely, wasting resources
            # 
            # SOLUTION: Early termination on zero results
            # - If current page has no links, year is empty
            # - No point in checking for pagination
            # - Exit immediately and move to next year
            # ═══════════════════════════════════════════════════════════════
            
            if links_found_on_page == 0:
                # No laws found on this page
                if page_count == 1:
                    # This is the first page with zero results
                    # The entire year has no laws
                    print(f"  ⚠️  No laws found for this year, skipping...")
                else:
                    # This shouldn't happen normally, but handle gracefully
                    print(f"  ⚠️  Empty page encountered on page {page_count}, stopping pagination")
                
                # CRITICAL: Break loop immediately
                # Do NOT attempt to find Next button on empty pages
                break
            
            # ───────────────────────────────────────────────────────────────
            # STEP 2: Look for the "Next" button (» symbol)
            # ───────────────────────────────────────────────────────────────
            # XPATH Strategy: //a[text()='»']
            # This finds any <a> tag whose text content is exactly '»'
            # ───────────────────────────────────────────────────────────────
            try:
                # Locate the Next button using XPATH
                next_button = driver.find_element(By.XPATH, "//a[text()='»']")
                
                # ───────────────────────────────────────────────────────────
                # STEP 3: Click the Next button
                # ───────────────────────────────────────────────────────────
                # Store current URL to detect page change
                old_url = driver.current_url
                
                # Click the button
                next_button.click()
                
                # ───────────────────────────────────────────────────────────
                # STEP 4: Wait for new page to load
                # ───────────────────────────────────────────────────────────
                # Strategy: Wait until URL changes (indicates navigation occurred)
                try:
                    WebDriverWait(driver, 10).until(
                        lambda d: d.current_url != old_url
                    )
                except:
                    pass
                
                # Additional wait for content to render
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "dettaglio"))
                    )
                except:
                    pass
                
                # Continue to next iteration (scrape next page)
                continue
                
            except:
                # ───────────────────────────────────────────────────────────
                # STEP 5: No Next button found - we're on the last page
                # ───────────────────────────────────────────────────────────
                # This is the normal exit condition
                break
        
    except Exception as e:
        # Handle any unexpected errors during pagination
        print(f"⚠️  Pagination error: {e}")
    
    finally:
        # Always close the driver to free resources
        if driver:
            driver.quit()
    
    return law_links

def save_excel():
    if ALL_DATA:
        df = pd.DataFrame(ALL_DATA)
        requested_columns = ["Region", "Law Title", "Law Number", "Date", "Filename"]
        final_cols = [c for c in requested_columns if c in df.columns]
        df = df[final_cols]
        df.to_excel(os.path.join(OUTPUT_FOLDER, EXCEL_FILENAME), index=False)

def main():
    print(f"📂 SAVING TO: {os.path.abspath(OUTPUT_FOLDER)}")
    print("🚀 MODE: HEADLESS=NEW (Stable) + 3 Workers")
    print("🔍 FILTER: Strict (Abrogata) Pattern Check ONLY")
    print("⚠️  RULE: Skip ONLY if pattern found in parentheses")
    
    years = get_year_list()
    if not years: return

    for y_info in tqdm(years, desc="Total Years"):
        links = get_links_for_year(y_info['url'])
        if not links: continue

        with tqdm(total=len(links), desc=f"   Processing {y_info['year']}", leave=False) as pbar:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(process_law_worker, link) for link in links]
                for future in as_completed(futures):
                    data, status = future.result()
                    pbar.update(1)
                    if data: ALL_DATA.append(data)
                    if len(ALL_DATA) % 10 == 0: save_excel()
        save_excel()
    
    print(f"\n✅ All Done! Files saved to: {os.path.abspath(OUTPUT_FOLDER)}")

if __name__ == "__main__":
    main()
    