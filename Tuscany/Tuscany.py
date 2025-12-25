import asyncio
import os
import re
import pandas as pd
import dateparser
from tqdm.asyncio import tqdm
from urllib.parse import urljoin
from playwright.async_api import async_playwright

BASE_URL = "https://raccoltanormativa.consiglio.regione.toscana.it/"
OUTPUT_DIR = "Toscana_Laws_PDFs"
EXCEL_FILE = "Toscana_Laws_Data.xlsx"
MAX_PARALLEL_TABS = 3

HEADERS = {
    "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/114.0 Safari/537.36"
}

results_data = []
processed_urls = set()
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ================= METADATA EXTRACTOR =================
def extract_metadata(text):
    text = text.lower().replace("\n", " ")

    # -------- Pattern A -------------
    # Legge regionale 8 gennaio 2025, n. 3
    pat1 = re.search(
        r'(\d{1,2}\s+'
        r'(gennaio|febbraio|marzo|aprile|maggio|giugno|luglio|agosto|settembre|ottobre|novembre|dicembre)'
        r'\s+\d{4}).*?(?:n\.|numero)\s*(\d+)',
        text,
        re.I
    )

    if pat1:
        date_str = pat1.group(1)
        law_number = pat1.group(3)
        dt = dateparser.parse(date_str, languages=["it"])

        iso = dt.strftime("%Y-%m-%d")
        readable = f"{dt.day} {dt.strftime('%B')} {dt.year}"
        return law_number, iso, readable

    # -------- Pattern B -------------
    # n. 3 del 08/01/2025
    pat2 = re.search(
        r'(?:n\.|numero)\s*(\d+)\s+del\s+(\d{2}/\d{2}/\d{4})',
        text
    )

    if pat2:
        law_number = pat2.group(1)
        date_str = pat2.group(2)
        dt = dateparser.parse(date_str, languages=["it"])

        iso = dt.strftime("%Y-%m-%d")
        readable = f"{dt.day} {dt.strftime('%B')} {dt.year}"
        return law_number, iso, readable

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
        print("⚠ Close Excel to update results.")


# ================= PROCESS EACH LAW =================
async def process_article(sem, context, url, pbar, counters):
    async with sem:
        page = await context.new_page()

        try:
            await page.goto(url, timeout=60000)

            page_text = await page.inner_text("body")
            law_number, iso_date, readable_date = extract_metadata(page_text)

            title = "Unknown Title"
            if await page.locator("#titolo_doc").count() > 0:
                title = await page.locator("#titolo_doc").inner_text()

            icon = "img[alt='Scarica il documento corrente in formato PDF']"
            if await page.locator(icon).count() == 0:
                counters["skipped"] += 1
                return

            async with page.expect_download() as dl:
                await page.click(icon)

            download = await dl.value
            temp_path = await download.path()

            final_name = f"Tuscany_{law_number}_{iso_date}.pdf"
            final_path = os.path.join(OUTPUT_DIR, final_name)
            os.replace(temp_path, final_path)

            counters["downloaded"] += 1

            results_data.append({
                "Region": "Toscana",
                "Law Title": title.strip(),
                "Law Number": law_number,
                "Date": readable_date,
                "Filename": final_name,
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
            headless=True   # <= 🔥🔥 HEADLESS MODE ENABLED
        )

        context = await browser.new_context(
            accept_downloads=True,
            user_agent=HEADERS["User-Agent"]
        )

        page = await context.new_page()
        await page.goto(BASE_URL)
        await page.wait_for_load_state("networkidle")

        menu_frame = None
        for f in page.frames:
            if await f.locator("img[name='j0_0']").count() > 0:
                menu_frame = f
                break

        if not menu_frame:
            menu_frame = page

        await menu_frame.click("img[name='j0_0']")
        await page.wait_for_timeout(1500)

        all_links = await menu_frame.locator("a").all()
        years = sorted(
            {(await l.inner_text()).strip()
             for l in all_links
             if (await l.inner_text()).strip().isdigit()},
            reverse=True
        )

        sem = asyncio.Semaphore(MAX_PARALLEL_TABS)
        counters = {"downloaded": 0, "skipped": 0, "failed": 0}

        for year in years:
            print(f"\nYEAR: {year}")

            xpath = f"//a[contains(text(), '{year}')]/preceding::img[contains(@name, 'j0_')][1]"
            icon = menu_frame.locator(f"xpath={xpath}")

            if await icon.count() == 0:
                continue

            await icon.first.click()
            await page.wait_for_timeout(2000)

            articles = menu_frame.locator("a[href*='articolo?urndoc=']")
            count = await articles.count()

            if count == 0:
                continue

            urls = []
            for i in range(count):
                href = await articles.nth(i).get_attribute("href")
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
