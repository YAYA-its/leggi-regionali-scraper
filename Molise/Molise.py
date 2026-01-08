import os
import re
import time
import pandas as pd
from playwright.sync_api import sync_playwright
from tqdm import tqdm

# ================= CONFIG =================

BASE_URL = "http://www1.regione.molise.it"
YEAR_URL_TEMPLATE = "http://www1.regione.molise.it/web/crm/lr.nsf/(a{})?Openview"

START_YEAR = 1979
END_YEAR = 2023

REGION = "Molise"
OUTPUT_DIR = "output_pdfs"
EXCEL_FILE = "molise_laws.xlsx"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ================= DATE / TEXT UTILS =================

ITALIAN_MONTHS = {
    "gennaio": "01",
    "febbraio": "02",
    "marzo": "03",
    "aprile": "04",
    "maggio": "05",
    "giugno": "06",
    "luglio": "07",
    "agosto": "08",
    "settembre": "09",
    "ottobre": "10",
    "novembre": "11",
    "dicembre": "12"
}

def clean_filename(text):
    """Remove invalid filename characters"""
    return re.sub(r'[\\/*?:"<>|]', "_", text)

def extract_date_from_text(text):
    """
    Extract Italian date from text with multiple pattern attempts.
    Returns: (date_text, date_iso) or (None, None)
    """
    if not text:
        return None, None
    
    text_lower = text.lower()
    
    # Pattern 1: "12 gennaio 2023"
    date_match = re.search(
        r"(\d{1,2})\s+"
        r"(gennaio|febbraio|marzo|aprile|maggio|giugno|luglio|agosto|settembre|ottobre|novembre|dicembre)\s+"
        r"(\d{4})",
        text_lower,
        re.IGNORECASE
    )
    
    if date_match:
        day = date_match.group(1).zfill(2)
        month_name = date_match.group(2).lower()
        year = date_match.group(3)
        month_num = ITALIAN_MONTHS.get(month_name, "01")
        
        date_text = f"{int(day)} {month_name.capitalize()} {year}"
        date_iso = f"{year}-{month_num}-{day}"
        return date_text, date_iso
    
    # Pattern 2: "12/01/2023" or "12-01-2023"
    date_match2 = re.search(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})", text)
    if date_match2:
        day = date_match2.group(1).zfill(2)
        month = date_match2.group(2).zfill(2)
        year = date_match2.group(3)
        
        # Convert to Italian month name if possible
        month_names = list(ITALIAN_MONTHS.keys())
        month_name = month_names[int(month)-1] if 1 <= int(month) <= 12 else "gennaio"
        
        date_text = f"{int(day)} {month_name.capitalize()} {year}"
        date_iso = f"{year}-{month}-{day}"
        return date_text, date_iso
    
    return None, None

def extract_law_number_from_text(text):
    """
    Extract law number with multiple pattern attempts.
    Returns: law_number or None
    """
    if not text:
        return None
    
    text_lower = text.lower()
    
    # Pattern 1: "n. 123" or "n.123" or "n 123"
    num_match = re.search(r"n\.?\s*(\d+)", text_lower)
    if num_match:
        return num_match.group(1)
    
    # Pattern 2: "legge 123" or "l. 123"
    num_match2 = re.search(r"(?:legge|l\.)\s+(\d+)", text_lower)
    if num_match2:
        return num_match2.group(1)
    
    # Pattern 3: Standalone number after "regionale"
    num_match3 = re.search(r"regionale\s+(\d+)", text_lower)
    if num_match3:
        return num_match3.group(1)
    
    return None

def extract_law_data_from_page(page):
    """
    Comprehensive extraction strategy:
    1. Try to extract from all visible text elements
    2. Prioritize specific structures (tables, lists)
    3. Use multiple regex patterns
    4. Aggregate results from multiple sources
    
    Returns: (law_number, date_text, date_iso, full_title)
    """
    
    law_number = None
    date_text = None
    date_iso = None
    full_title = ""
    
    # Strategy 1: Extract from ALL text content on the page
    try:
        page_text = page.locator("body").inner_text()
        
        # Try to find law number
        if not law_number:
            law_number = extract_law_number_from_text(page_text)
        
        # Try to find date
        if not date_text:
            date_text, date_iso = extract_date_from_text(page_text)
    except Exception as e:
        print(f"Warning: Could not extract from body text: {e}")
    
    # Strategy 2: Look for structured data in tables
    try:
        tables = page.locator("table").all()
        for table in tables:
            table_text = table.inner_text()
            
            if not law_number:
                law_number = extract_law_number_from_text(table_text)
            
            if not date_text:
                date_text, date_iso = extract_date_from_text(table_text)
            
            # Extract potential title from table cells
            cells = table.locator("td").all()
            for cell in cells:
                cell_text = cell.inner_text().strip()
                if len(cell_text) > len(full_title) and len(cell_text) > 40:
                    full_title = cell_text
    except Exception as e:
        print(f"Warning: Could not extract from tables: {e}")
    
    # Strategy 3: Look in list items
    try:
        list_items = page.locator("li").all()
        for li in list_items:
            li_text = li.inner_text().strip()
            
            if "legge regionale" in li_text.lower():
                if not law_number:
                    law_number = extract_law_number_from_text(li_text)
                
                if not date_text:
                    date_text, date_iso = extract_date_from_text(li_text)
                
                if len(li_text) > len(full_title):
                    full_title = li_text
    except Exception as e:
        print(f"Warning: Could not extract from list items: {e}")
    
    # Strategy 4: Look at all paragraphs
    try:
        paragraphs = page.locator("p").all()
        for p in paragraphs:
            p_text = p.inner_text().strip()
            
            if not law_number:
                law_number = extract_law_number_from_text(p_text)
            
            if not date_text:
                date_text, date_iso = extract_date_from_text(p_text)
            
            if len(p_text) > len(full_title) and len(p_text) > 40:
                full_title = p_text
    except Exception as e:
        print(f"Warning: Could not extract from paragraphs: {e}")
    
    # Strategy 5: Check headers
    try:
        headers = page.locator("h1, h2, h3, h4").all()
        for h in headers:
            h_text = h.inner_text().strip()
            
            if not law_number:
                law_number = extract_law_number_from_text(h_text)
            
            if not date_text:
                date_text, date_iso = extract_date_from_text(h_text)
            
            if len(h_text) > len(full_title):
                full_title = h_text
    except Exception as e:
        print(f"Warning: Could not extract from headers: {e}")
    
    # Fallback: Use page title if no title found
    if not full_title:
        full_title = page.title()
    
    # Final fallback values
    law_number = law_number if law_number else "NA"
    date_text = date_text if date_text else "NA"
    date_iso = date_iso if date_iso else "NA"
    
    return law_number, date_text, date_iso, full_title

# ================= MAIN =================

def main():
    print("\n🔵 STARTING MOLISE HEADFUL SCRAPER (1979 → 2023)\n")

    if os.path.exists(EXCEL_FILE):
        df = pd.read_excel(EXCEL_FILE)
        print(f"📊 Loaded existing Excel with {len(df)} records")
    else:
        df = pd.DataFrame(columns=[
            "Region", "Law Title", "Law Number", "Date", "Date_ISO", "Filename"
        ])

    years = list(range(END_YEAR, START_YEAR - 1, -1))

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            slow_mo=200
        )

        context = browser.new_context(
            viewport={"width": 1400, "height": 900}
        )
        page = context.new_page()

        law_queue = []

        # ================= COLLECT LAW LINKS =================

        print("📋 Phase 1: Collecting law links...\n")
        for year in years:
            year_url = YEAR_URL_TEMPLATE.format(year)
            print(f"➡ Processing year {year}")

            try:
                page.goto(year_url, timeout=60000)
                page.wait_for_load_state("networkidle")
            except Exception as e:
                print(f"❌ Error loading year {year}: {e}")
                continue

            page_count = 1
            while True:
                links = page.locator("a[href*='OpenDocument']").all()
                print(f"   📄 Page {page_count}: Found {len(links)} laws")
                
                for a in links:
                    href = a.get_attribute("href")
                    title = a.inner_text().strip()
                    if href:
                        law_queue.append({
                            "url": BASE_URL + href,
                            "fallback_title": title
                        })

                next_btn = page.locator("a:has-text('Successiva')")
                if next_btn.count() == 0:
                    break

                try:
                    next_btn.first.click()
                    page.wait_for_load_state("networkidle")
                    time.sleep(0.4)
                    page_count += 1
                except Exception as e:
                    print(f"   ⚠️ No more pages or error: {e}")
                    break

        print(f"\n📄 TOTAL LAWS FOUND: {len(law_queue)}\n")

        # ================= DOWNLOAD PDFs =================

        print("💾 Phase 2: Downloading and processing PDFs...\n")
        progress = tqdm(law_queue, desc="Processing Laws")

        stats = {"success": 0, "missing_number": 0, "missing_date": 0, "skipped": 0}

        for item in progress:
            url = item["url"]
            fallback_title = item["fallback_title"]

            try:
                page.goto(url, timeout=60000)
                page.wait_for_load_state("networkidle")
                time.sleep(0.3)  # Brief pause for dynamic content
            except Exception as e:
                print(f"\n❌ Error loading {url}: {e}")
                continue

            # ENHANCED EXTRACTION
            law_number, date_text, date_iso, title = extract_law_data_from_page(page)
            
            # Use fallback title if extraction failed
            if not title or title == "NA":
                title = fallback_title

            # Update statistics
            if law_number == "NA":
                stats["missing_number"] += 1
            if date_iso == "NA":
                stats["missing_date"] += 1
            if law_number != "NA" and date_iso != "NA":
                stats["success"] += 1

            # Create filename
            filename = clean_filename(f"{REGION}_{law_number}_{date_iso}.pdf")
            filepath = os.path.join(OUTPUT_DIR, filename)

            # Skip if already exists
            if os.path.exists(filepath):
                stats["skipped"] += 1
                continue

            # Save PDF
            try:
                page.pdf(
                    path=filepath,
                    format="A4",
                    print_background=True
                )
            except Exception as e:
                print(f"\n❌ Error saving PDF {filename}: {e}")
                continue

            # Add to dataframe
            df.loc[len(df)] = [
                REGION,
                title,
                law_number,
                date_text,
                date_iso,
                filename
            ]

            # Crash-safe incremental write
            try:
                df.to_excel(EXCEL_FILE, index=False)
            except Exception as e:
                print(f"\n⚠️ Warning: Could not save Excel: {e}")

        browser.close()

    # ================= FINAL REPORT =================

    print("\n" + "="*70)
    print("✅ SCRAPING COMPLETE")
    print("="*70)
    print(f"📊 Statistics:")
    print(f"   • Total laws processed: {len(law_queue)}")
    print(f"   • Successfully extracted: {stats['success']}")
    print(f"   • Missing law numbers: {stats['missing_number']}")
    print(f"   • Missing dates: {stats['missing_date']}")
    print(f"   • Skipped (already exist): {stats['skipped']}")
    print(f"\n📁 Output files:")
    print(f"   • PDFs: {OUTPUT_DIR}/")
    print(f"   • Excel: {EXCEL_FILE}")
    print("="*70 + "\n")

if __name__ == "__main__":
    main()