import os
import time
import base64
import re
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

BASE_URL = "https://www.consiglio.regione.lazio.it/?vw=leggiregionali&sv=vigente"
OUTPUT_FOLDER = os.path.join(os.getcwd(), "Lazio_Laws_Final")
EXCEL_FILENAME = "Lazio_Laws_Index.xlsx"
TEST_MODE_LIMIT = 251  # Set to a high number to scrape all available laws

def setup_driver():
    """Initializes Chrome in Headless mode."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--log-level=3")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

def sanitize_filename_keep_spaces(text):
    """
    Cleans text to be safe for filenames.
    Preserves spaces so '9 dicembre 2025' remains '9 dicembre 2025'.
    Replaces slashes with dashes and removes illegal filename characters (* : ? \" < > |).
    """
    if not text:
        return "Unknown"
    # Replace slashes with dashes (e.g. 10/12/2025 -> 10-12-2025)
    text = text.replace("/", "-").replace("\\", "-")
    # Remove illegal filename characters (* : ? " < > |)
    text = re.sub(r'[*:?"<>|]', "", text)
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def save_page_as_pdf(driver, filepath):
    """Uses Chrome to print the current page text to PDF."""
    try:
        result = driver.execute_cdp_cmd("Page.printToPDF", {
            "printBackground": True,
            "paperWidth": 8.27, "paperHeight": 11.69,
            "marginTop": 0.5, "marginBottom": 0.5,
            "displayHeaderFooter": False
        })
        with open(filepath, "wb") as f:
            f.write(base64.b64decode(result['data']))
        return True
    except Exception as e:
        print(f"   ❌ PDF Save Error: {e}")
        return False

def scrape_metadata(driver):
    """
    Extracts raw text:
    - Number: "7" from label "Numero della legge: 7" or "Numero: 7"
    - Date: "26 giugno 2025" from label "Data: 26 giugno 2025"
    Keeps the Italian textual date form intact.
    """
    data = {"Title": "N/A", "Number": "N/A", "Date": "N/A"}
    try:
        # 1. Title - try header tags first
        for tag in ("h1", "h2", "h3", "h4"):
            try:
                el = driver.find_element(By.TAG_NAME, tag)
                txt = el.text.strip()
                if txt:
                    data["Title"] = txt
                    break
            except Exception:
                continue

        body_text = ""
        try:
            body_text = driver.find_element(By.TAG_NAME, "body").text
        except Exception:
            pass

        # 2. Numero della legge - prefer strong label then fallback to search in body
        try:
            # Look for strong that mentions 'Numero' (common patterns)
            num_elem = driver.find_element(By.XPATH,
                "//strong[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'numero') "
                "or contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'numero della legge')]")
            parent_text = ""
            try:
                parent = num_elem.find_element(By.XPATH, "..")
                parent_text = parent.text.strip()
            except Exception:
                parent_text = num_elem.text.strip()

            if ":" in parent_text:
                num_val = parent_text.split(":", 1)[1].strip()
                # sometimes there is extra text; extract first numeric token
                m = re.search(r"([0-9]{1,5})", num_val)
                data["Number"] = m.group(1) if m else num_val
            else:
                # fallback to digits in the parent text
                m = re.search(r"([0-9]{1,5})", parent_text)
                if m:
                    data["Number"] = m.group(1)
        except Exception:
            # fallback: search body text for label 'Numero' or 'Numero della legge'
            try:
                m = re.search(r"Numero(?: della legge)?[:\s]*([0-9]{1,5})", body_text, flags=re.IGNORECASE)
                if m:
                    data["Number"] = m.group(1)
            except Exception:
                pass

        # 3. Date - prefer strong label 'Data' but avoid 'BUR' matches
        try:
            date_elem = driver.find_element(By.XPATH,
                "//strong[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'data') "
                "and not(contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'bur'))]")
            # Use parent's full text because label and value are often in the same block
            parent_text = ""
            try:
                parent = date_elem.find_element(By.XPATH, "..")
                parent_text = parent.text.strip()
            except Exception:
                parent_text = date_elem.text.strip()

            if ":" in parent_text:
                date_val = parent_text.split(":", 1)[1].strip()
                data["Date"] = date_val if date_val else "N/A"
            else:
                # fallback to find date-like patterns in parent_text
                m = re.search(r"([0-3]?\d\s+[A-Za-zàèéìòù]+\s+\d{4})", parent_text, flags=re.IGNORECASE)
                if m:
                    data["Date"] = m.group(1)
        except Exception:
            # fallback: search whole body for 'Data: <...>' or Italian textual date
            try:
                m = re.search(r"Data[:\s]*([0-3]?\d\s+[A-Za-zàèéìòù]+\s+\d{4}|[0-3]?\d[\/\-][0-1]?\d[\/\-]\d{2,4})", body_text, flags=re.IGNORECASE)
                if m:
                    data["Date"] = m.group(1)
            except Exception:
                pass

    except Exception:
        pass

    # Ensure defaults
    for k in ("Title", "Number", "Date"):
        if not data.get(k):
            data[k] = "N/A"

    return data

def main():
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
        print(f"📂 Output Folder: {OUTPUT_FOLDER}")
        print(f"⚙️  Limit: {TEST_MODE_LIMIT} pages.")

    driver = setup_driver()
    all_laws_data = []

    # Master list to prevent infinite loops
    all_unique_urls = []

    try:
        # --- PHASE 1: LINK COLLECTION ---
        print(f"🔍 PHASE 1: Scanning for laws...")
        page_num = 1

        while True:
            # 1. TEST LIMIT CHECK
            if page_num > TEST_MODE_LIMIT:
                print(f"   🛑 Reached limit of {TEST_MODE_LIMIT} pages. Stopping scan.")
                break

            url = f"{BASE_URL}&pg={page_num}"
            print(f"   Scanning Page {page_num}...", end="")
            driver.get(url)
            time.sleep(1.5)

            # Find links
            link_elements = driver.find_elements(By.XPATH, "//a[contains(@href, 'leggiregionalidettaglio') and contains(@href, 'id=')]")
            current_page_links = [link.get_attribute('href') for link in link_elements if link.get_attribute('href')]

            # 2. EMPTY PAGE CHECK
            if not current_page_links:
                print(" -> No links found. Finished.")
                break

            # 3. DUPLICATE CONTENT CHECK (Fixes Page 11 loop)
            new_links_on_this_page = 0
            for href in current_page_links:
                if href not in all_unique_urls:
                    all_unique_urls.append(href)
                    new_links_on_this_page += 1

            print(f" -> Found {new_links_on_this_page} new laws.")

            if new_links_on_this_page == 0:
                print("   🛑 No new laws found (Content repeated). STOPPING.")
                break

            page_num += 1

        print(f"✅ Found {len(all_unique_urls)} unique laws in total.\n")

        # --- PHASE 2: PROCESSING ---
        print("💾 PHASE 2: Downloading & Renaming...")

        for index, url in enumerate(all_unique_urls):
            print(f"[{index+1}/{len(all_unique_urls)}] Processing: {url}")
            driver.get(url)
            time.sleep(1.5)

            meta = scrape_metadata(driver)

            # Sanitize text (keep spaces, fix slashes)
            law_num_raw = meta["Number"] if meta["Number"] and meta["Number"] != "N/A" else None
            law_date_raw = meta["Date"] if meta["Date"] and meta["Date"] != "N/A" else None

            if not law_num_raw:
                # fallback to id from URL
                try:
                    law_id = url.split("id=")[1].split("&")[0]
                    law_num_raw = f"ID_{law_id}"
                except Exception:
                    law_num_raw = "Unknown"

            # Keep original Italian date text exactly as-is (sanitized only)
            safe_law_num = sanitize_filename_keep_spaces(law_num_raw)
            safe_law_date = sanitize_filename_keep_spaces(law_date_raw) if law_date_raw else "unknown_date"

            # --- FILENAME CREATION ---
            # Result: Lazio_19_9 dicembre 2025.pdf
            filename = f"Lazio_{safe_law_num}_{safe_law_date}.pdf"
            full_path = os.path.join(OUTPUT_FOLDER, filename)

            if save_page_as_pdf(driver, full_path):
                all_laws_data.append({
                    "Law Title": meta["Title"],
                    "Law Number": meta["Number"],
                    "Date": meta["Date"],
                    "Filename": filename,
                    "URL": url
                })
            else:
                print("   ⚠️ Failed to save PDF. Saving HTML snapshot instead.")
                # save HTML as fallback and record that filename
                html_name = f"Lazio_{safe_law_num}_{safe_law_date}.html"
                html_path = os.path.join(OUTPUT_FOLDER, html_name)
                try:
                    with open(html_path, "w", encoding="utf-8") as f:
                        f.write(driver.page_source)
                    all_laws_data.append({
                        "Law Title": meta["Title"],
                        "Law Number": meta["Number"],
                        "Date": meta["Date"],
                        "Filename": html_name,
                        "URL": url
                    })
                except Exception as e:
                    print(f"   ❌ Could not save HTML fallback: {e}")

    except Exception as e:
        print(f"\n❌ Critical Error: {e}")

    finally:
        driver.quit()

        # --- PHASE 3: SAVE EXCEL ---
        if all_laws_data:
            print("\n📊 PHASE 3: Saving Excel Index...")
            df = pd.DataFrame(all_laws_data)
            excel_path = os.path.join(OUTPUT_FOLDER, EXCEL_FILENAME)
            df = df[["Law Title", "Law Number", "Date", "Filename", "URL"]]
            df.to_excel(excel_path, index=False)
            print(f"✅ Done! Excel saved to: {excel_path}")

if __name__ == "__main__":
    main()
