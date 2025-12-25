import os
import time
import re
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# --- CONFIGURATION ---
URL = "https://atticonsiglio.consiglio.basilicata.it/AD_Elenco_Leggi"
OUTPUT_FOLDER = os.path.abspath("Basilicata_Laws_PDFs") 
EXCEL_FILENAME = "Basilicata_Laws_Data.xlsx"
REGION_NAME = "Basilicata"
SAVE_FREQUENCY = 5

if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

def clean_text(text):
    return " ".join(text.split()).strip() if text else ""

def get_row_data(cols):
    law_num = "0"
    date_iso = "0000-00-00"
    excel_date = "Unknown"
    law_title = "Unknown"
    
    text_candidates = []
    
    for col in cols:
        text = clean_text(col.text)
        if not text: continue
        
        # 1. Date (dd/mm/yyyy)
        if re.search(r"\d{2}/\d{2}/\d{4}", text):
            match = re.search(r"(\d{2})/(\d{2})/(\d{4})", text)
            if match:
                day, month, year = match.groups()
                date_iso = f"{year}-{month}-{day}"
                excel_date = f"{day}/{month}/{year}"
            continue 

        # 2. Number Detection
        # Finds "50" or "n. 50", ignores dates
        if len(text) < 15:
            digit_search = re.search(r"(\d+)", text)
            if digit_search and not re.search(r"/", text):
                if re.match(r"^(n\.? ?)?\d+$", text.lower()) or text.isdigit():
                     law_num = digit_search.group(1)
                     continue

        # 3. Title Candidates
        text_candidates.append(text)

    if text_candidates:
        law_title = max(text_candidates, key=len)

    return law_num, date_iso, excel_date, law_title

def force_rename(folder, target_name, timeout=30):
    """Watches folder for new file and renames it."""
    end_time = time.time() + timeout
    existing_files = set(os.listdir(folder))
    
    while time.time() < end_time:
        current_files = set(os.listdir(folder))
        new_files = current_files - existing_files
        
        for f in new_files:
            if f.endswith('.crdownload') or f.endswith('.tmp'):
                continue
            
            try:
                original_path = os.path.join(folder, f)
                target_path = os.path.join(folder, target_name)
                
                if os.path.exists(target_path):
                    os.remove(target_path)
                
                os.rename(original_path, target_path)
                
                if os.path.exists(target_path):
                    return True
            except:
                time.sleep(1)
                continue
        time.sleep(0.5)
    return False

def check_and_restore_page(driver, wait, target_page):
    """
    Ensures we are on 'target_page'.
    If we are lost (e.g. back on page 1), it navigates back.
    """
    if target_page == 1: return
    
    try:
        # 1. Try finding specific number link "11"
        links = driver.find_elements(By.XPATH, f"//table//tr//td//a[text()='{target_page}']")
        if links:
            print(f" > Restoring Page {target_page} (Clicked Number)...")
            links[0].click()
            wait.until(EC.staleness_of(links[0]))
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table[id*='GVElenco']")))
            return

        # 2. If number not found, we might need to click "..." to see it
        # This is tricky for restoration, usually we just need to click '...' forward
        # For simplicity, if we can't find the exact page number, we warn the user.
        # But usually, 'Back' button issues only happen within the visible range.
        
    except Exception as e:
        print(f"Warning restoring page: {e}")

def main():
    chrome_options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": OUTPUT_FOLDER,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True 
    }
    chrome_options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    data_list = []
    current_page = 1
    
    try:
        print(f"Opening {URL} ...")
        driver.get(URL)
        wait = WebDriverWait(driver, 15)
        
        while True:
            print(f"\n====== PROCESSING PAGE {current_page} ======")
            
            # Wait for grid
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table[id*='GVElenco']")))
            table = driver.find_element(By.CSS_SELECTOR, "table[id*='GVElenco']")
            all_rows = table.find_elements(By.TAG_NAME, "tr")
            
            # Find Data Rows
            data_indices = [i for i, r in enumerate(all_rows) if r.find_elements(By.CSS_SELECTOR, "input[src*='Btn_Selezione.png']")]
            
            print(f"Found {len(data_indices)} laws.")
            
            # Loop Rows
            for i in range(len(data_indices)):
                retries = 3
                while retries > 0:
                    try:
                        # Restore Page if needed
                        if current_page > 1:
                            check_and_restore_page(driver, wait, current_page)

                        # Re-locate elements
                        table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table[id*='GVElenco']")))
                        rows = table.find_elements(By.TAG_NAME, "tr")
                        current_row = rows[data_indices[i]]
                        cols = current_row.find_elements(By.TAG_NAME, "td")
                        
                        law_num, date_iso, excel_date, law_title = get_row_data(cols)
                        filename = f"{REGION_NAME}_{law_num}_{date_iso}.pdf"
                        
                        print(f"[{i+1}] {law_num} | {date_iso}", end=" ... ")

                        # Check/Download
                        target_path = os.path.join(OUTPUT_FOLDER, filename)
                        
                        if os.path.exists(target_path):
                            print("Exists.")
                            status = filename
                        else:
                            select_btn = current_row.find_element(By.CSS_SELECTOR, "input[src*='Btn_Selezione.png']")
                            select_btn.click()
                            time.sleep(1.5)
                            
                            try:
                                dl_btn = driver.find_element(By.CSS_SELECTOR, "a[id*='LnkLeggeDownload']")
                                dl_btn.click()
                                
                                if force_rename(OUTPUT_FOLDER, filename):
                                    print("Downloaded!")
                                    status = filename
                                else:
                                    print("Rename Failed.")
                                    status = "Error"
                            except:
                                print("No Button.")
                                status = "No Button"

                            driver.back()
                            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table[id*='GVElenco']")))

                        data_list.append({
                            "Region": REGION_NAME,
                            "Law Title": law_title,
                            "Law Number": law_num,
                            "Date": excel_date,
                            "Filename": status
                        })
                        
                        if len(data_list) % SAVE_FREQUENCY == 0:
                            pd.DataFrame(data_list).to_excel(EXCEL_FILENAME, index=False)
                        
                        break # Done with this row

                    except Exception:
                        driver.get(URL) # Reset
                        retries -= 1

            # --- PAGINATION LOGIC (FIXED) ---
            next_page = current_page + 1
            print(f"Looking for Page {next_page}...")
            
            if current_page > 1: check_and_restore_page(driver, wait, current_page)
            
            # Strategy 1: Look for exact number "11"
            next_links = driver.find_elements(By.XPATH, f"//table//tr//td//a[text()='{next_page}']")
            
            if next_links:
                print(f"Found specific link for Page {next_page}. Clicking...")
                next_links[0].click()
                current_page += 1
                time.sleep(2)
            else:
                # Strategy 2: Look for "..." (The Next Block button)
                print(f"Specific link '{next_page}' not found. Checking for '...' (Ellipsis)...")
                ellipsis_links = driver.find_elements(By.XPATH, "//table//tr//td//a[text()='...']")
                
                if ellipsis_links:
                    # Usually the LAST '...' is the "Next" button (First might be "Previous")
                    # We click the last one found.
                    print("Found '...' button. Clicking to load next set of pages...")
                    ellipsis_links[-1].click()
                    current_page += 1
                    time.sleep(2)
                else:
                    print("NO more pages found (No number link and no ellipsis). Finished!")
                    break

    finally:
        df = pd.DataFrame(data_list)
        df = df[["Region", "Law Title", "Law Number", "Date", "Filename"]]
        df.to_excel(EXCEL_FILENAME, index=False)
        print("Done.")
        driver.quit()

if __name__ == "__main__":
    main()