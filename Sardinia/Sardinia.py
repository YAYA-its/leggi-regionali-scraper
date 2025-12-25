import os
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from tqdm import tqdm  # Library for progress bar

# --- CONFIGURATION ---
URL = "https://www.consregsardegna.it/leggi-approvate-xvii-legislatura/"
DOWNLOAD_FOLDER = "Sardinia_Laws_PDFs"
EXCEL_FILENAME = "Sardinia_Laws_Metadata.xlsx"
REGION_NAME = "Sardinia"

# Italian to Number Month Mapping (Only used for Filename generation)
MONTH_MAP = {
    'gennaio': '01', 'febbraio': '02', 'marzo': '03',
    'aprile': '04', 'maggio': '05', 'giugno': '06',
    'luglio': '07', 'agosto': '08', 'settembre': '09',
    'ottobre': '10', 'novembre': '11', 'dicembre': '12'
}

def clean_filename(text):
    """Removes illegal characters from filenames."""
    return re.sub(r'[\\/*?:"<>|]', "", text)

def get_iso_date_from_italian(date_str):
    """
    Parses '1° dicembre 2025' -> Returns '2025-12-01' (for filename).
    Returns None if parsing fails.
    """
    try:
        # Clean up "1°" to "1"
        clean_str = date_str.replace("1°", "1").lower()
        parts = clean_str.split()
        
        if len(parts) >= 3:
            day = parts[0].zfill(2) # Ensure 01, 02 etc.
            month_txt = parts[1]
            year = parts[2]
            
            if month_txt in MONTH_MAP:
                month_num = MONTH_MAP[month_txt]
                return f"{year}-{month_num}-{day}"
    except Exception:
        return None
    return None

def scrape_laws():
    # Create download directory
    if not os.path.exists(DOWNLOAD_FOLDER):
        os.makedirs(DOWNLOAD_FOLDER)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    print(f"Fetching {URL}...")
    try:
        response = requests.get(URL, headers=headers)
        response.raise_for_status()
    except Exception as e:
        print(f"Failed to load page: {e}")
        return

    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Find all potential PDF links
    links = [a for a in soup.find_all('a', href=True) if a['href'].lower().endswith('.pdf')]
    
    print(f"Found {len(links)} PDF links. Starting extraction...")

    extracted_data = []

    # --- PROGRESS BAR LOOP ---
    # tqdm wraps the list 'links' to create a progress bar
    for link in tqdm(links, desc="Processing Laws", unit="file"):
        href = link['href']
        link_text = link.get_text().strip()
        
        # --- REGEX EXTRACTION ---
        # 1. Try: "Legge regionale 1° dicembre 2025, n. 33"
        match = re.search(r'Legge regionale\s+(.*?),\s*n\.\s*(\d+)', link_text, re.IGNORECASE)
        
        if not match:
            # 2. Try: "Legge regionale n. 33 del 1° dicembre 2025"
            match = re.search(r'Legge regionale\s+n\.\s*(\d+)\s+del\s+(.*)', link_text, re.IGNORECASE)
            if match:
                law_num = match.group(1)
                law_date_it = match.group(2).strip()
            else:
                continue # Skip if patterns don't match
        else:
            law_date_it = match.group(1).strip()
            law_num = match.group(2)

        # --- FILENAME GENERATION ---
        # Requirement: Region_LawNum_Date.pdf (YYYY-MM-DD)
        iso_date = get_iso_date_from_italian(law_date_it)
        
        if iso_date:
            date_for_filename = iso_date
        else:
            date_for_filename = "Unknown_Date"

        new_filename = f"{REGION_NAME}_{law_num}_{date_for_filename}.pdf"
        new_filename = clean_filename(new_filename)

        # --- METADATA ---
        full_title = link_text
        parent = link.parent
        if parent.name == 'p':
            full_title = " ".join(parent.get_text().strip().split())

        row = {
            "Region": REGION_NAME,
            "Law Title": full_title,
            "Law Number": int(law_num),
            "Date": law_date_it,  # Keeps original Italian text (e.g., "9 dicembre 2025")
            "Filename": new_filename
        }
        extracted_data.append(row)

        # --- DOWNLOAD PDF ---
        pdf_path = os.path.join(DOWNLOAD_FOLDER, new_filename)
        if not os.path.exists(pdf_path):
            try:
                pdf_resp = requests.get(href, headers=headers, stream=True)
                with open(pdf_path, 'wb') as f:
                    for chunk in pdf_resp.iter_content(chunk_size=8192):
                        f.write(chunk)
            except Exception as e:
                # If download fails, we still record metadata but log error
                # You can choose to break or continue. Here we continue.
                pass 
        
        # --- FREQUENT SAVING ---
        # Save to Excel after every iteration to ensure no data loss
        try:
            df = pd.DataFrame(extracted_data)
            df.to_excel(EXCEL_FILENAME, index=False)
        except Exception as e:
            # If Excel is open, it might fail to write.
            pass

    print(f"\nCompleted! Scraped {len(extracted_data)} laws.")
    print(f"Metadata saved to: {EXCEL_FILENAME}")

if __name__ == "__main__":
    scrape_laws()