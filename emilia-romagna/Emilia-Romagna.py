import os
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from datetime import datetime

# --- CONFIGURATION ---
BASE_URL = "https://demetra.regione.emilia-romagna.it/al/"
START_URL = "https://demetra.regione.emilia-romagna.it/al/hit-page?id_wgt=query_lr&hlist=first&src=1&src_f=1"
OUTPUT_DIR = "Downloaded_Laws"
EXCEL_FILENAME = "Emilia_Romagna_Laws.xlsx"
MAX_WORKERS = 5  # Number of parallel downloads
SAVE_INTERVAL = 10  # Save Excel every N items

# Italian Month Mapping
MONTH_MAP = {
    'gennaio': '01', 'febbraio': '02', 'marzo': '03', 'aprile': '04',
    'maggio': '05', 'giugno': '06', 'luglio': '07', 'agosto': '08',
    'settembre': '09', 'ottobre': '10', 'novembre': '11', 'dicembre': '12'
}

# --- HELPER FUNCTIONS ---

def parse_italian_date(date_str):
    """
    Converts '25 luglio 2025' to ('2025-07-25', '25 luglio 2025')
    Returns: (formatted_date_for_filename, clean_date_for_excel)
    """
    try:
        # Clean up string
        clean_date = date_str.strip()
        parts = clean_date.split()
        
        if len(parts) >= 3:
            day = parts[0].zfill(2)
            month_txt = parts[1].lower()
            year = parts[2]
            
            # Remove comma if present in year (e.g. "2025,")
            year = year.replace(',', '').replace('.', '')
            
            if month_txt in MONTH_MAP:
                month_num = MONTH_MAP[month_txt]
                formatted_date = f"{year}-{month_num}-{day}"
                return formatted_date, f"{day} {month_txt} {year}"
                
    except Exception:
        pass
    
    # Fallback if parsing fails
    return "0000-00-00", date_str

def clean_filename(text):
    """Removes illegal characters from filenames."""
    return re.sub(r'[\\/*?:"<>|]', "", text)

def download_file(url, filepath, pbar_dl):
    """Downloads a file and updates the progress bar."""
    try:
        if os.path.exists(filepath):
            pbar_dl.update(1)
            return "Skipped (Exists)"

        response = requests.get(url, stream=True, timeout=30)
        if response.status_code == 200:
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            pbar_dl.update(1)
            return "Downloaded"
        else:
            pbar_dl.update(1)
            return f"Failed (Status {response.status_code})"
    except Exception as e:
        pbar_dl.update(1)
        return f"Failed ({str(e)})"

# --- MAIN SCRAPER ---

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })

    # Data storage
    all_data = []
    
    # Progress Bars
    # We don't know the total pages initially, so we use total=None for pages
    pbar_pages = tqdm(desc="Pages Scanned", unit="pg", position=0)
    pbar_scrape = tqdm(desc="Laws Found   ", unit="law", position=1)
    pbar_dl = tqdm(desc="PDFs Processed ", unit="file", position=2)

    current_url = START_URL
    page_count = 0
    
    # Executor for parallel downloads
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    future_to_data = {}

    try:
        while current_url:
            page_count += 1
            try:
                response = session.get(current_url)
                soup = BeautifulSoup(response.content, 'html.parser')
            except Exception as e:
                tqdm.write(f"Error fetching page: {e}")
                break

            # Find law items (Assuming rows are in a standard table or div structure)
            # Based on the HTML snippet provided, we look for the download links directly 
            # or the container. Usually, these are in a table or list.
            # We will iterate through all download buttons to find the context.
            
            # Strategy: Find all 'Access document' buttons
            # Selector based on user snippet: class="hitelement_button"
            download_links = soup.find_all('a', class_='hitelement_button', title=True)
            
            if not download_links:
                tqdm.write("No documents found on this page.")
            
            for link in download_links:
                pbar_scrape.update(1)
                
                # --- EXTRACTION ---
                title_attr = link.get('title', '') # e.g., "Accedi al documento Legge regionale 25 luglio 2025, n.9"
                full_text = title_attr.replace("Accedi al documento ", "")
                
                # Default values
                region = "Emilia-Romagna" # Hardcoded as per domain
                law_num = "Unknown"
                law_date_clean = "Unknown"
                law_date_fmt = "0000-00-00"
                
                # Regex to extract Date and Number
                # Matches: "Legge regionale 25 luglio 2025, n.9"
                # Pattern: Date (group 1), Number (group 2)
                match = re.search(r'Legge regionale\s+(.*?),\s+n\.(\d+)', full_text, re.IGNORECASE)
                
                if match:
                    raw_date = match.group(1) # "25 luglio 2025"
                    law_num = match.group(2)  # "9"
                    
                    law_date_fmt, law_date_clean = parse_italian_date(raw_date)
                else:
                    # Fallback for different formats
                    pass

                # Construct Filename: Region_LawNum_Date.pdf
                filename = f"{region}_{law_num}_{law_date_fmt}.pdf"
                filename = clean_filename(filename)
                filepath = os.path.join(OUTPUT_DIR, filename)

                # Construct Download Link
                href = link.get('href')
                if href and not href.startswith('http'):
                    pdf_url = BASE_URL + href
                else:
                    pdf_url = href

                # --- METADATA DICT ---
                row_data = {
                    "Region": region,
                    "Law Title": full_text,
                    "Law Number": law_num,
                    "Date": law_date_clean,
                    "Filename": filename,
                    "Status": "Pending"
                }
                
                # Submit download task
                future = executor.submit(download_file, pdf_url, filepath, pbar_dl)
                future_to_data[future] = row_data

            # --- PROCESS FINISHED DOWNLOADS (BATCH UPDATE) ---
            # We check for completed futures periodically to update the Excel list
            done_futures = [f for f in future_to_data if f.done() and not future_to_data[f].get('processed')]
            
            for f in done_futures:
                result_status = f.result()
                data = future_to_data[f]
                data['Status'] = result_status
                data['processed'] = True # Mark as handled
                all_data.append(data)

            # Save Excel periodically
            if len(all_data) % SAVE_INTERVAL == 0 and len(all_data) > 0:
                df = pd.DataFrame(all_data)
                # Remove internal flag
                save_df = df.drop(columns=['processed'], errors='ignore')
                save_df.to_excel(EXCEL_FILENAME, index=False)

            # --- PAGINATION ---
            # Look for "Succ. >>"
            # <a href="..." ... > Succ. >></a>
            next_link = soup.find('a', string=re.compile(r'Succ\.'))
            
            if next_link:
                next_href = next_link.get('href')
                current_url = BASE_URL + next_href
                pbar_pages.update(1)
            else:
                current_url = None # Stop loop

    except KeyboardInterrupt:
        tqdm.write("\nProcess interrupted by user. Saving current progress...")

    # --- FINAL CLEANUP ---
    tqdm.write("\nWaiting for remaining downloads to complete...")
    for f in as_completed(future_to_data):
        if not future_to_data[f].get('processed'):
            data = future_to_data[f]
            data['Status'] = f.result()
            all_data.append(data)

    # Final Save
    if all_data:
        df = pd.DataFrame(all_data)
        if 'processed' in df.columns:
            df = df.drop(columns=['processed'])
        df.to_excel(EXCEL_FILENAME, index=False)
        tqdm.write(f"\nCompleted. Data saved to {EXCEL_FILENAME}")
    else:
        tqdm.write("No data found.")

    pbar_pages.close()
    pbar_scrape.close()
    pbar_dl.close()
    executor.shutdown()

if __name__ == "__main__":
    main()