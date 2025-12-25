import os
import re
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# --- CONFIGURATION ---
MAIN_PAGE_URL = "https://lexview-int.regione.fvg.it/FontiNormative/xml/Materia.aspx"
OUTPUT_DIR = os.path.join(os.getcwd(), "FVG_Laws_PDFs")
EXCEL_FILE = "FVG_Laws_Metadata_Final.xlsx"

TARGET_CATEGORIES = [] 

# --- BROWSER SETUP ---
chrome_options = Options()
# Enable Headless Mode
chrome_options.add_argument("--headless=new") 
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

prefs = {
    "download.default_directory": OUTPUT_DIR,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "plugins.always_open_pdf_externally": True
}
chrome_options.add_experimental_option("prefs", prefs)

def clean_text(text):
    if not text: return ""
    return re.sub(r'\s+', ' ', text.replace(u'\xa0', u' ')).strip()

def get_formatted_date(date_str):
    """
    Input: "25 marzo 1968"
    Output: "25/03/1968"
    """
    if not date_str: return "UnknownDate"
    
    clean_str = clean_text(date_str)
    
    month_map = {
        'gennaio': '01', 'febbraio': '02', 'marzo': '03', 'aprile': '04',
        'maggio': '05', 'giugno': '06', 'luglio': '07', 'agosto': '08',
        'settembre': '09', 'ottobre': '10', 'novembre': '11', 'dicembre': '12'
    }

    pattern = r"(\d{1,2})[\s\.\-\/]+([a-zA-Z]+)[\s\.\-\/]+(\d{4})"
    match = re.search(pattern, clean_str, re.IGNORECASE)
    
    if match:
        day, month_name, year = match.groups()
        month_num = month_map.get(month_name.lower())
        if month_num:
            return f"{day.zfill(2)}/{month_num}/{year}"

    return clean_str

def handle_cookie_consent(driver):
    try:
        accept_btn = WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Accetta') or contains(text(), 'OK')]"))
        )
        driver.execute_script("arguments[0].click();", accept_btn)
        time.sleep(1)
    except:
        pass

def wait_for_download_and_rename(law_num, formatted_date):
    """
    Waits for download and renames.
    Replaces '/' with '-' for the filename only.
    """
    time.sleep(1.5)
    end_time = time.time() + 30
    
    # Safe filename: 25/03/1968 -> 25-03-1968
    safe_date_for_file = formatted_date.replace('/', '-')
    
    while time.time() < end_time:
        files = sorted([os.path.join(OUTPUT_DIR, f) for f in os.listdir(OUTPUT_DIR)], key=os.path.getmtime)
        if files:
            latest_file = files[-1]
            if not latest_file.endswith(".crdownload") and not latest_file.endswith(".tmp"):
                try:
                    region = "FVG"
                    new_filename = f"{region}_{law_num}_{safe_date_for_file}.pdf"
                    new_filepath = os.path.join(OUTPUT_DIR, new_filename)
                    if os.path.exists(new_filepath): os.remove(new_filepath)
                    os.rename(latest_file, new_filepath)
                    return new_filename
                except:
                    time.sleep(1)
                    continue
        time.sleep(1)
    return None

def save_metadata(data):
    df_new = pd.DataFrame([data])
    if os.path.exists(EXCEL_FILE):
        try:
            with pd.ExcelWriter(EXCEL_FILE, mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:
                df_new.to_excel(writer, startrow=writer.sheets['Sheet1'].max_row, header=False, index=False)
        except:
            df_new.to_excel(EXCEL_FILE, index=False)
    else:
        df_new.to_excel(EXCEL_FILE, index=False)

def process_single_law(driver, law_url, law_date_raw, category_name):
    original_window = driver.current_window_handle
    driver.switch_to.new_window('tab')
    driver.get(law_url)
    
    status = "Failed"
    filename = ""
    title = ""
    law_num = "0"
    
    final_date_str = get_formatted_date(law_date_raw)
    print(f"     > Date Debug: Raw='{law_date_raw}' -> Formatted='{final_date_str}'")

    try:
        wait = WebDriverWait(driver, 10)
        
        try:
            xpath_title = "//span[@id='PageBody_lbOggettolegge' or contains(@id, 'lblOggetto') or contains(@id, 'lblTitolo')]"
            title_el = driver.find_element(By.XPATH, xpath_title)
            title = clean_text(title_el.text)
        except:
            title = "Title Not Found"

        try:
            num_el = driver.find_element(By.ID, "PageBody_lbNumLegge")
            law_num = clean_text(num_el.text)
        except:
            num_match = re.search(r"n\.\s*(\d+)", title)
            if num_match: law_num = num_match.group(1)

        wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(@href, 'xmlLex.aspx')]"))).click()
        wait.until(EC.element_to_be_clickable((By.XPATH, "//a[@id='PageBody_aPdf' or text()='PDF']"))).click()
        
        filename = wait_for_download_and_rename(law_num, final_date_str)
        status = "Success" if filename else "Timeout"

    except Exception as e:
        status = f"Error: {str(e)[:30]}"
    finally:
        driver.close()
        driver.switch_to.window(original_window)

    if status == "Success":
        print(f"     [OK] {filename}")
        save_metadata({
            'Category': category_name,
            'Region': 'Friuli Venezia Giulia',
            'Law Title': title,
            'Law Number': law_num,
            'Date': final_date_str, 
            'Filename': filename,
            'Status': status
        })

def set_view_to_all(driver):
    print("   > Setting view to 'tutti' (Show All)...")
    try:
        select_element = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.NAME, "selectoroscopo"))
        )
        select = Select(select_element)
        select.select_by_visible_text("tutti")
        WebDriverWait(driver, 10).until(EC.staleness_of(select_element))
        time.sleep(3)
        return True
    except Exception as e:
        print(f"   > 'Show All' failed/not found: {str(e)[:50]}")
        return False

def run_scraper():
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    try:
        print(">>> Loading Main Page (Headless Mode)...")
        driver.get(MAIN_PAGE_URL)
        handle_cookie_consent(driver)

        cat_elements = driver.find_elements(By.XPATH, "//a[contains(@href, 'Lista.aspx?materia=')]")
        categories = []
        for el in cat_elements:
            name = clean_text(el.text)
            url = el.get_attribute('href')
            if TARGET_CATEGORIES and name not in TARGET_CATEGORIES: continue
            categories.append({'name': name, 'url': url})
        
        print(f">>> Found {len(categories)} categories.")

        for cat in categories:
            print(f"\n>>> Category: {cat['name']}")
            driver.get(cat['url'])
            handle_cookie_consent(driver)
            
            set_view_to_all(driver)
            
            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//table[contains(@id, 'gvListaRegolamenti')]")))
                rows = driver.find_elements(By.XPATH, "//table[contains(@id, 'gvListaRegolamenti')]//tr")
            except:
                print("   > No laws found.")
                continue

            laws_to_process = []
            for row in rows[1:]:
                try:
                    if not row.find_elements(By.XPATH, ".//a[contains(@href, 'IndiceLex')]"): continue
                    link = row.find_element(By.XPATH, ".//a[contains(@href, 'IndiceLex')]")
                    date_span = row.find_element(By.XPATH, ".//span[contains(@id, 'lblData')]")
                    date_text = date_span.get_attribute("textContent").strip()
                    laws_to_process.append({'url': link.get_attribute('href'), 'date': date_text})
                except: continue

            print(f"   > Found {len(laws_to_process)} laws total. Processing...")

            for i, law in enumerate(laws_to_process, 1):
                if i % 10 == 0: print(f"     Processing {i}/{len(laws_to_process)}...")
                process_single_law(driver, law['url'], law['date'], cat['name'])

    finally:
        print("\n>>> Done.")

if __name__ == "__main__":
    run_scraper()