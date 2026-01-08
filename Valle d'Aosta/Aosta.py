import os
import re
import asyncio
import pandas as pd
from playwright.async_api import async_playwright, TimeoutError
from urllib.parse import urlparse, parse_qs, urljoin

# ================= CONFIGURATION =================
BASE_URL = "https://www.consiglio.vda.it/app/leggieregolamenti/"
SEARCH_URL = "https://www.consiglio.vda.it/app/leggieregolamenti/risultatiricerca"

OUTPUT_DIR = os.path.abspath("VDA_Laws_PDFs")
EXCEL_FILE = "VDA_Laws_Data.xlsx"

START_YEAR = 2025
END_YEAR = 1950
MAX_WORKERS = 3
NAV_TIMEOUT = 15000      # Hard navigation timeout
MAX_PAGES_PER_YEAR = 50  # Pagination safety guard

MONTH_MAP = {
    'gennaio': '01', 'febbraio': '02', 'marzo': '03', 'aprile': '04',
    'maggio': '05', 'giugno': '06', 'luglio': '07', 'agosto': '08',
    'settembre': '09', 'ottobre': '10', 'novembre': '11', 'dicembre': '12'
}

# ================= ABROGATION DETECTION =================
# These exact substrings indicate a law has been repealed/abrogated
ABROGATION_INDICATORS = [
    "(abrogato dalla l.r.",
    "(abrogata dall'art.",
    "(legge abrogata dall'art",
    "(abrogato dall'art.",
    "(regolamento abrogato dall'art.",
    "(abrogata, a decorrere dal",
    "(Abrogata dal"
]

# ================= UTILS =================

def get_law_id(url):
    """
    Extract primary key (pk_lr) from URL query parameters.
    
    Args:
        url (str): Full URL containing pk_lr parameter
        
    Returns:
        str: Law ID or "0" if extraction fails
    """
    try:
        return parse_qs(urlparse(url).query).get('pk_lr', ['0'])[0]
    except:
        return "0"

def is_law_abrogated(page_text):
    """
    Determine if a law has been abrogated.
    
    Includes normalization to handle:
    1. Case sensitivity (converts to lowercase).
    2. Smart/Curled Apostrophes (replaces ’ with ').
    """
    if not page_text:
        return False
    
    # 1. Convert entire text to lowercase to match our indicators list
    text_clean = page_text.lower()
    
    # 2. CRITICAL FIX: Replace "Smart Quotes" (’) with standard quotes (')
    # The website often uses typographic apostrophes which cause exact matches to fail
    text_clean = text_clean.replace("’", "'").replace("`", "'").replace("‘", "'")
    
    # 3. Check for indicators in the cleaned text
    for indicator in ABROGATION_INDICATORS:
        if indicator in text_clean:
            return True
    
    return False

def parse_metadata(header_text):
    """
    Extract structured date and law number from Italian-formatted header text.
    
    Handles variations like:
    - "15° gennaio 2023, n. 12"
    - "3 marzo 2020, numero 5"
    
    Args:
        header_text (str): Raw header text from webpage element
        
    Returns:
        tuple: (clean_date, iso_date, law_num)
            - clean_date (str): Human-readable format "15 gennaio 2023"
            - iso_date (str): ISO format "2023-01-15" for sorting
            - law_num (str): Extracted law number or "0"
    """
    clean_date = "Unknown"
    iso_date = "0000-00-00"
    law_num = "0"

    try:
        # Match Italian date format: "15° gennaio 2023"
        date_match = re.search(
            r'(\d{1,2}°?)\s+(gennaio|febbraio|marzo|aprile|maggio|giugno|luglio|agosto|settembre|ottobre|novembre|dicembre)\s+(\d{4})',
            header_text,
            re.IGNORECASE
        )

        if date_match:
            day_raw, month_txt, year = date_match.groups()
            day = day_raw.replace('°', '')
            month_num = MONTH_MAP.get(month_txt.lower(), '01')
            clean_date = f"{day} {month_txt} {year}"
            iso_date = f"{year}-{month_num}-{int(day):02d}"

        # Match law number: "n. 12", "num. 5", "numero 8"
        num_match = re.search(r'(?:n\.|num\.|numero)\s*(\d+)', header_text, re.IGNORECASE)
        if num_match:
            law_num = num_match.group(1)

    except:
        pass

    return clean_date, iso_date, law_num

# ================= CORE WORKER FUNCTION =================

async def process_law(context, law_data, semaphore, all_results):
    """
    Process a single law with strict abrogation validation gate.
    
    CRITICAL EXECUTION ORDER:
    ┌─────────────────────────────────────────────────────────────┐
    │ 1. Navigate to law detail page                              │
    │ 2. Extract full page text (document.body.innerText)         │
    │ 3. ⛔ ABROGATION CHECK (GATE)                               │
    │    ├─ IF ABROGATED:                                         │
    │    │   ├─ Print skip message                                │
    │    │   ├─ Delete existing PDF (cleanup)                     │
    │    │   └─ RETURN (no download, no Excel entry)             │
    │    └─ ELSE (Valid law):                                     │
    │        ├─ Check if PDF already exists                       │
    │        ├─ Download PDF if needed                            │
    │        └─ Save metadata to Excel                            │
    └─────────────────────────────────────────────────────────────┘
    
    Args:
        context: Playwright browser context for page creation
        law_data (dict): Contains url, title, number, date, filename
        semaphore: Asyncio semaphore for concurrency limiting
        all_results (list): Thread-safe list for Excel data accumulation
        
    Returns:
        None: Modifies all_results list in-place
    """
    async with semaphore:
        page = await context.new_page()

        link = law_data['url']
        filename = law_data['filename']
        filepath = os.path.join(OUTPUT_DIR, filename)

        # Prepare result dictionary (only saved for non-abrogated laws)
        result = {
            'Region': "Valle d'Aosta",
            'Law Title': law_data['title'],
            'Law Number': law_data['number'],
            'Date': law_data['date'],
            'Filename': filename,
            'Status': 'Failed',
            'URL': link
        }

        try:
            # ═══════════════════════════════════════════════════════
            # PHASE 1: NAVIGATION & TEXT EXTRACTION
            # ═══════════════════════════════════════════════════════
            await page.goto(link, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
            await page.wait_for_timeout(300)  # Allow dynamic content to render

            # Extract complete page text for abrogation analysis
            page_text = await page.evaluate("() => document.body.innerText")

            # ═══════════════════════════════════════════════════════
            # PHASE 2: ABROGATION VALIDATION (CRITICAL GATE)
            # ═══════════════════════════════════════════════════════
            if is_law_abrogated(page_text):
                # Law is abrogated - immediate termination without download
                
                law_id = f"{law_data['title'][:60]}... (#{law_data['number']})"
                print(f"  ⊘ Skipping abrogated law: {law_id}")
                
                # Cleanup: Remove PDF if exists from previous runs (before filter was added)
                if os.path.exists(filepath):
                    try:
                        os.remove(filepath)
                        print(f"     └─ Deleted old PDF: {filename}")
                    except Exception as del_err:
                        print(f"     └─ Could not delete {filename}: {del_err}")
                
                # CRITICAL: Early return - prevent download and Excel entry
                await page.close()
                return
            
            # ═══════════════════════════════════════════════════════
            # PHASE 3: VALID LAW PROCESSING (Only reached if NOT abrogated)
            # ═══════════════════════════════════════════════════════
            
            # Check if PDF already exists (avoid re-downloading)
            if os.path.exists(filepath):
                result['Status'] = 'Skipped'
                all_results.append(result)
                await page.close()
                return

            # ═══════════════════════════════════════════════════════
            # PHASE 4: PDF DOWNLOAD (Only for valid, new laws)
            # ═══════════════════════════════════════════════════════
            
            # Disable browser print dialog (prevents popup interference)
            await page.evaluate("window.print = function() {}")

            # Attempt to trigger print-optimized layout
            try:
                btn = page.locator("button[onclick*='window.print']")
                if await btn.count() > 0:
                    await btn.click(timeout=2000)
                    await page.wait_for_timeout(500)
            except:
                pass  # Print button optional - continue without it

            # Generate PDF file
            await page.pdf(path=filepath, format="A4", print_background=True)
            result['Status'] = 'Downloaded'
            all_results.append(result)

        except Exception as e:
            result['Status'] = f"Error: {e}"
            all_results.append(result)

        finally:

            await page.close()

# ================= MAIN ORCHESTRATION =================

async def main():
    """
    Main scraping orchestration with year-by-year processing.
    
    Architecture:
    - Single browser instance shared across all years
    - Context refresh every 5 years (prevents memory leaks)
    - Sequential year processing (2025 → 1950)
    - Concurrent law processing within each year (MAX_WORKERS=3)
    - Incremental Excel saves after each year (crash recovery)
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    all_results = []

    # Load existing Excel data for resume capability
    if os.path.exists(EXCEL_FILE):
        try:
            all_results = pd.read_excel(EXCEL_FILE).to_dict('records')
        except:
            pass

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()

        for idx, year in enumerate(range(START_YEAR, END_YEAR - 1, -1)):
            print(f"\n--- Scanning Year {year} ---")

            # Refresh context every 5 years (prevents slowdown)
            if idx > 0 and idx % 5 == 0:
                await context.close()
                context = await browser.new_context()

            page = await context.new_page()
            laws_to_process = []
            page_num = 1

            # ═══════════════════════════════════════════════════════
            # COLLECTION PHASE: Scrape search results for law URLs
            # ═══════════════════════════════════════════════════════
            while True:
                if page_num > MAX_PAGES_PER_YEAR:
                    print(f"Pagination stop safeguard for {year}")
                    break

                url = f"{SEARCH_URL}?tipo=&numero_legge=&anno={year}&ricerca_in=1&pagina={page_num}"

                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
                    await page.wait_for_timeout(400)

                    link_elements = page.locator("a[href*='dettaglio?pk_lr=']")
                    count = await link_elements.count()

                    if count == 0:
                        break

                    new_found = False

                    for i in range(count):
                        el = link_elements.nth(i)
                        raw_href = await el.get_attribute("href")
                        full_url = urljoin(page.url, raw_href)

                        header_text = (await el.text_content() or "").strip()

                        desc_loc = el.locator(
                            "xpath=./following::p[contains(@class,'elemento_lista_paginata_banche_dati')][1]"
                        )
                        title_text = (await desc_loc.text_content() if await desc_loc.count() else header_text)

                        clean_date, iso_date, law_num = parse_metadata(header_text)
                        pk_id = get_law_id(full_url)

                        filename = (
                            f"ValleAosta_{law_num}_{iso_date}.pdf"
                            if iso_date != "0000-00-00"
                            else f"ValleAosta_ID_{pk_id}.pdf"
                        )

                        if not any(l['url'] == full_url for l in laws_to_process):
                            laws_to_process.append({
                                'url': full_url,
                                'date': clean_date,
                                'number': law_num,
                                'title': title_text.strip()[:1000],
                                'filename': filename
                            })
                            new_found = True

                    if not new_found and page_num > 1:
                        break

                    page_num += 1

                except TimeoutError:
                    print(f"Timeout year {year} page {page_num} → skipping page")
                    page_num += 1
                    continue

                except Exception as e:
                    print(f"Pagination error {year} page {page_num}: {e}")
                    page_num += 1
                    continue

            await page.close()

            if not laws_to_process:
                print(f"No laws found for {year}")
                continue

            print(f"Found {len(laws_to_process)} laws → downloading")

            # ═══════════════════════════════════════════════════════
            # PROCESSING PHASE: Concurrent law validation & download
            # ═══════════════════════════════════════════════════════
            sem = asyncio.Semaphore(MAX_WORKERS)
            tasks = [process_law(context, law, sem, all_results) for law in laws_to_process]
            await asyncio.gather(*tasks)

            # ═══════════════════════════════════════════════════════
            # PERSISTENCE PHASE: Save results after each year
            # ═══════════════════════════════════════════════════════
            df = pd.DataFrame(all_results)
            cols = ['Region', 'Law Title', 'Law Number', 'Date', 'Filename', 'Status', 'URL']
            for c in cols:
                if c not in df.columns:
                    df[c] = ""

            df[cols].to_excel(EXCEL_FILE, index=False)
            print(f"✓ Saved year {year}")

        await browser.close()
        print("\n--- ALL YEARS COMPLETED ---")

if __name__ == "__main__":
    asyncio.run(main())