import asyncio
import os
import re
import pandas as pd
from tqdm.asyncio import tqdm
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# --- CONFIGURATION ---
BASE_URL = "https://normelombardia.consiglio.regione.lombardia.it/Accessibile/main.aspx"
OUTPUT_DIR = "Lombardia_Laws_PDFs"
EXCEL_FILE = "Lombardia_Laws_Data.xlsx"
MAX_RETRIES = 3

HEADERS = {
    "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115 Safari/537.36"
}

ITALIAN_MONTHS = {
    "gennaio": "01", "febbraio": "02", "marzo": "03", "aprile": "04",
    "maggio": "05", "giugno": "06", "luglio": "07", "agosto": "08",
    "settembre": "09", "ottobre": "10", "novembre": "11", "dicembre": "12"
}

results = []
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ---------- UTILS ----------
def extract_from_list_text(text):
    txt = text.lower().strip()
    m = re.search(
        r'(\d{1,2})\s+'
        r'(gennaio|febbraio|marzo|aprile|maggio|giugno|luglio|agosto|settembre|ottobre|novembre|dicembre)'
        r'\s+(\d{4}).*?n\.?\s*(\d+)',
        txt
    )
    if not m:
        return "Unknown", "0000-00-00", ""

    day = m.group(1).zfill(2)
    month_name = m.group(2)
    year = m.group(3)
    law = m.group(4)
    month = ITALIAN_MONTHS[month_name]
    iso = f"{year}-{month}-{day}"
    readable = f"{int(day)} {month_name} {year}"
    return law, iso, readable


def save_excel():
    if not results:
        return
    df = pd.DataFrame(results)
    df.to_excel(EXCEL_FILE, index=False)


# ---------- DEEP TREE EXPANDER ----------
async def expand_everything(page):
    """
    Kay-b9a y-cliquer 3la 'Apri cartella' 7tta mkayb9a walo masdoud.
    """
    print("📂 STARTING DEEP EXPANSION (This might take 1-2 minutes)...")
    
    iteration = 0
    while True:
        iteration += 1
        folders = page.locator("img.icona_albero[alt='Apri cartella']")
        count = await folders.count()
        
        if count == 0:
            print("✅ Tree fully expanded.")
            break
            
        print(f"   ↳ Round {iteration}: Opening {count} folders...")
        
        for i in range(count):
            try:
                await folders.first.click()
                await page.wait_for_timeout(200) 
            except:
                pass
        
        await page.wait_for_timeout(1000)
        
        if iteration > 50:
            print("⚠️ Stopped expansion after 50 rounds (Safety Limit).")
            break


# ---------- PROCESS LAW ----------
async def process_law(context, item, pbar, counters, year, index):
    url = item["url"]
    law_no = item["law_no"]
    iso = item["iso"]
    readable_date = item["readable"]
    title_text = item["title"]

    filename = f"Lombardia_{law_no}_{iso}.pdf"
    final_path = os.path.join(OUTPUT_DIR, filename)

    if os.path.exists(final_path):
        counters["skipped"] += 1
        pbar.update(1)
        return

    for attempt in range(1, MAX_RETRIES + 1):
        page = await context.new_page()
        try:
            await page.goto(url, timeout=60000)

            pdf_btn = page.locator("a[href^='esportaDoc.aspx?type=pdf']")
            
            if await pdf_btn.count() == 0:
                counters["skipped"] += 1
                await page.close()
                break

            async with page.expect_download(timeout=60000) as dl:
                await pdf_btn.first.click()

            download = await dl.value
            await download.save_as(final_path)
            
            counters["downloaded"] += 1
            results.append({
                "Region": "Lombardia",
                "Law Title": title_text.strip(),
                "Law Number": law_no,
                "Date": readable_date,
                "Filename": filename,
                "Source URL": url
            })

            if len(results) % 5 == 0:
                save_excel()

            await page.close()
            break 

        except Exception:
            await page.close()
            if attempt < MAX_RETRIES:
                await asyncio.sleep(2)
            else:
                counters["failed"] += 1

    pbar.update(1)
    pbar.set_description(f"Year {year} | DL {counters['downloaded']}")


# ---------- MAIN ----------
async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(accept_downloads=True, user_agent=HEADERS["User-Agent"])
        page = await context.new_page()
        
        print("🌍 Connecting to Lombardia Main Page...")
        await page.goto(BASE_URL, timeout=120000)

        # Cookie
        try:
            cookie = page.locator("button, a", has_text="Accetta")
            if await cookie.count() > 0:
                await cookie.first.click()
        except:
            pass

        # 1. EXPAND EVERYTHING
        await expand_everything(page)

        # 2. IDENTIFY YEARS (INTEGERS ONLY)
        print("🔍 Scanning tree for available years...")
        all_tree_links = page.locator("a.treelink, a.treelinkselected")
        count_links = await all_tree_links.count()
        
        found_years = set()
        
        for i in range(count_links):
            text = await all_tree_links.nth(i).inner_text()
            match = re.search(r'\b(19\d{2}|20\d{2})\b', text)
            if match:
                y = int(match.group(1))
                if 1970 <= y <= 2025:
                    found_years.add(y)

        sorted_years = sorted(list(found_years), reverse=True)
        print(f"✅ Found {len(sorted_years)} years visible: {sorted_years}")

        counters = {"downloaded": 0, "skipped": 0, "failed": 0}
        base = page.url.rsplit("/", 1)[0] + "/"

        # 3. PROCESS EACH YEAR (FRESH LOOKUP)
        for year in sorted_years:
            print(f"\n==== YEAR {year} ====")
            
            # 👇 FIX: Re-find the element FRESHLY every time
            # We look for a link that CONTAINS the year (e.g. "2024" or "2024 (50)")
            # Regex ensures we match strictly the year number
            try:
                year_regex = re.compile(rf"\b{year}\b")
                link_element = page.locator("a.treelink, a.treelinkselected").filter(has_text=year_regex).first
                
                # Check visibility
                if await link_element.count() == 0:
                    print(f"⚠️ Year {year} element disappeared. Tree might have collapsed.")
                    # Fallback: Try expanding again if lost
                    await expand_everything(page)
                    link_element = page.locator("a.treelink, a.treelinkselected").filter(has_text=year_regex).first

                await link_element.scroll_into_view_if_needed()
                await link_element.click()
                
            except Exception as e:
                print(f"❌ Could not click year {year}: {e}")
                continue

            # Wait for results to load
            try:
                await page.locator("a[href*='view=showdoc']").first.wait_for(timeout=10000)
            except PlaywrightTimeoutError:
                print(f"No laws visible for {year} (or timed out loading list)")
                continue

            law_links = page.locator("a[href*='view=showdoc']")
            count = await law_links.count()
            
            if count == 0:
                print(f"No laws in {year}")
                continue
            
            urls = []
            for i in range(count):
                el = law_links.nth(i)
                href = await el.get_attribute("href")
                text = await el.inner_text()
                if href and "javascript" not in href:
                    law_no, iso, readable = extract_from_list_text(text)
                    urls.append({
                        "url": base + href,
                        "law_no": law_no,
                        "iso": iso,
                        "readable": readable,
                        "title": text
                    })

            print(f"Found {len(urls)} laws in {year}")
            
            pbar = tqdm(total=len(urls), desc=f"Year {year}", leave=True)
            for idx, u in enumerate(urls, start=1):
                await process_law(context, u, pbar, counters, year, idx)
            pbar.close()

        save_excel()
        print("\nDONE 🎯")
        print(counters)
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())