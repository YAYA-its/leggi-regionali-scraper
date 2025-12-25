import asyncio
import re
import os
import pandas as pd
from playwright.async_api import async_playwright
from tqdm.asyncio import tqdm
from urllib.parse import urljoin
import dateparser

# --- CONFIGURATION ---
BASE_URL = "https://lrv.regione.liguria.it/liguriass_prod/"
OUTPUT_DIR = "Liguria_Laws_PDFs"
EXCEL_FILE = "Liguria_Laws_Data.xlsx"
MAX_PARALLEL_TABS = 3

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/115 Safari/537.36"
}

results_data = []
processed_urls = set()

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


# ================= METADATA EXTRACTOR =================
def extract_metadata(text):
    text = text.lower().replace("\n", " ")

    pat1 = re.search(
        r'(\d{1,2}\s+'
        r'(gennaio|febbraio|marzo|aprile|maggio|giugno|luglio|agosto|settembre|ottobre|novembre|dicembre)'
        r'\s+\d{4}).*?(?:n\.|numero)\s*(\d+)',
        text,
        re.I
    )

    if pat1:
        dt = dateparser.parse(pat1.group(1), languages=["it"])
        return pat1.group(3), dt.strftime("%Y-%m-%d"), f"{dt.day} {dt.strftime('%B')} {dt.year}"

    pat2 = re.search(
        r'(?:n\.|numero)\s*(\d+)\s+del\s+(\d{2}/\d{2}/\d{4})',
        text
    )

    if pat2:
        dt = dateparser.parse(pat2.group(2), languages=["it"])
        return pat2.group(1), dt.strftime("%Y-%m-%d"), f"{dt.day} {dt.strftime('%B')} {dt.year}"

    return "Unknown", "0000-00-00", ""


# ================= SAVE EXCEL =================
def save_excel():
    if not results_data:
        return

    df = pd.DataFrame(results_data)
    cols = ["Region", "Law Title", "Law Number", "Date", "Filename", "Source URL"]

    for c in cols:
        if c not in df.columns:
            df[c] = ""

    df = df[cols]
    try:
        df.to_excel(EXCEL_FILE, index=False)
    except PermissionError:
        print("⚠ Close Excel to update results")


# ================= PROCESS EACH LAW PAGE =================
async def process_article(sem, context, url, pbar, counters):
    async with sem:
        page = await context.new_page()

        try:
            await page.goto(url, timeout=60000)

            text = await page.inner_text("body")
            law_number, iso_date, readable_date = extract_metadata(text)

            title = "Unknown Title"
            if await page.locator("h1").count() > 0:
                title = await page.locator("h1").first.inner_text()

            pdf_icon = page.locator("img[src*='pdf'], img[alt*='PDF'], a[href*='.pdf']")
            if await pdf_icon.count() == 0:
                counters["skipped"] += 1
                return

            async with page.expect_download() as dl:
                await pdf_icon.first.click()

            download = await dl.value
            temp_path = await download.path()

            final_filename = f"Liguria_{law_number}_{iso_date}.pdf"
            final_path = os.path.join(OUTPUT_DIR, final_filename)
            os.replace(temp_path, final_path)

            counters["downloaded"] += 1

            results_data.append({
                "Region": "Liguria",
                "Law Title": title.strip(),
                "Law Number": law_number,
                "Date": readable_date,
                "Filename": final_filename,
                "Source URL": url
            })

            if len(results_data) % 5 == 0:
                save_excel()

            pbar.update(1)
            pbar.set_description(f"Downloaded {counters['downloaded']}")

        except Exception as e:
            counters["failed"] += 1
            print("ERROR:", e)

        finally:
            await page.close()


# ================= MAIN =================
async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True    # 👈 HEADLESS ENABLED
        )

        context = await browser.new_context(
            accept_downloads=True,
            user_agent=HEADERS["User-Agent"]
        )

        page = await context.new_page()
        await page.goto(BASE_URL)
        await page.wait_for_load_state("networkidle")

        # 1️⃣ CLICK ROOT EXPAND
        root_plus = page.locator("img[name='j0_0']")
        await root_plus.click()
        await page.wait_for_timeout(2000)

        # 2️⃣ COLLECT YEARS LIKE TUSCANY
        all_links = await page.locator("a").all()
        years = []

        for link in all_links:
            try:
                text = (await link.inner_text()).strip()
                if text.isdigit() and 1950 < int(text) <= 2050:
                    years.append(text)
            except:
                pass

        years = sorted(list(set(years)), reverse=True)
        print(f"FOUND YEARS: {years}")

        sem = asyncio.Semaphore(MAX_PARALLEL_TABS)
        counters = {"downloaded": 0, "skipped": 0, "failed": 0}

        for year in years:
            print(f"\n>>> YEAR: {year}")

            xpath = f"//a[contains(text(), '{year}')]/preceding::img[contains(@name, 'j0_')][1]"
            icon = page.locator(f"xpath={xpath}")

            if await icon.count() == 0:
                continue

            await icon.first.click()
            await page.wait_for_timeout(2500)

            articles = page.locator("a[href*='articolo'], a[href*='view'], a[href*='id=']")
            count = await articles.count()

            if count == 0:
                continue

            urls = []

            for i in range(count):
                href = await articles.nth(i).get_attribute("href")
                if not href or "javascript" in href:
                    continue

                full = urljoin(BASE_URL, href)

                if full not in processed_urls:
                    processed_urls.add(full)
                    urls.append(full)

            pbar = tqdm(total=len(urls), desc=f"Year {year}", leave=False)

            tasks = [
                process_article(sem, context, u, pbar, counters)
                for u in urls
            ]

            await asyncio.gather(*tasks)
            pbar.close()

        save_excel()
        print("\nDONE 🎯")
        print(counters)

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
