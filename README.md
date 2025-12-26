# Web Scraping Suite for Italian Regional Laws (Leggi Regionali)

## Project Overview

This project is a comprehensive suite of independent web scraping scripts designed to extract legislative data from the Official Regional Bulletins (BUR - Bollettino Ufficiale Regionale) of various Italian regions.

The repository contains approximately 20 distinct scrapers, each tailored to the specific structure and requirements of a region's official portal. The primary goal is to automate the retrieval of regional laws, regulations, and administrative acts, downloading the associated PDF documents and extracting metadata into structured formats (CSV/Excel).

## Key Features

-   **Automated Data Extraction**: Crawls regional portals to harvest law metadata (titles, dates, issue numbers).
-   **PDF Downloading**: Automatically detects and downloads the full text of laws in PDF format.
-   **Structured Export**: Exports collected data to Excel (`.xlsx`) or CSV formats for easy analysis and archiving.
-   **Robust Error Handling**: Scripts are designed to handle network interruptions and portal inconsistencies.
-   **Progress Tracking**: Uses `tqdm` to show real-time progress during long scraping sessions.

## Tech Stack

The project is built using **Python** and leverages a powerful set of libraries for web scraping and data manipulation:

-   **Web Scraping & Browser Automation**:
    -   `Selenium` & `WebDriver Manager`: For rendering JavaScript-heavy portals and simulating user interactions.
    -   `Playwright`: For handling complex, modern web applications.
    -   `Requests` & `BeautifulSoup4`: For efficient HTTP requests and HTML parsing of static pages.
-   **Data Processing**:
    -   `Pandas`: For data cleaning, organization, and DataFrame operations.
    -   `OpenPyxl`: For writing rich Excel files.
    -   `Dateparser`: For robust parsing of Italian date formats.
-   **Utilities**:
    -   `tqdm`: For progress bars.
    -   `Concurrent.futures` (Standard Lib): For parallel processing in supported scripts.

## Project Structure

The repository is organized by region. Each folder contains the specific scraper for that region, keeping dependencies and logic isolated where necessary.

```text
leggi-regionali-scraper/
├── BASILICATA/
│   └── Basilicata.py
├── Calabria/
│   └── Calabria.py
├── emilia-romagna/
│   └── Emilia-Romagna.py
├── Friuli-Venezia/
│   └── Friuli-Venezia Giulia.py
├── Lazio/
│   └── Lazio.py
├── Liguria/
│   └── Liguria.py
├── Lombardia/
│   └── Lombardia.py
├── Piemonte/
│   └── Piemonte.py
├── Puglia/
│   └── Apulia.py
├── Sardinia/
│   └── Sardinia.py
├── Sicily/
│   └── Sicily.py
├── Tuscany/
│   └── Tuscany.py
├── Umbria/
│   └── Umbria.py
├── veneto/
│   └── Veneto.py
├── ... (other regions)
├── requirements.txt   # Project dependencies
```

## Setup & Usage

### 1. Prerequisites

Ensure you have Python installed (3.8+ recommended).

### 2. Set up the Environment

It is recommended to use a virtual environment to manage dependencies.

**Create a virtual environment:**
```bash
python -m venv venv
```

**Activate the virtual environment:**
-   **Windows (PowerShell):**
    ```powershell
    .\venv\Scripts\Activate
    ```
-   **macOS/Linux:**
    ```bash
    source venv/bin/activate
    ```

### 3. Install Dependencies

Install the required Python packages:

```bash
pip install -r requirements.txt
```

*Note: If using scripts that rely on Playwright, install the browser binaries:*
```bash
playwright install
```

### 4. Running a Scraper

Navigate to the region's folder or run the script directly from the root.

**Example: Scraping Veneto**
```bash
python veneto/Veneto.py
```

**Example: Scraping Tuscany**
```bash
python Tuscany/Tuscany.py
```

The script will launch, perform the scraping, and save the output (Excel/PDFs) in the respective directory or a configured output folder.
