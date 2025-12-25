# Jiji Scrapper

A lightweight, **Scrapy + Playwright** based web-scraper for extracting listings from the popular **Jiji.com.gh** site.
The project provides two spiders:

1.  **UrlSpider** – crawls search results and collects the URLs of individual listings.
2.  **ListingSpider** – visits each URL and extracts detailed listing information (title, price, location, specs, amenities, etc.).

---

## Features

- **Dynamic page handling** with `scrapy-playwright` (JavaScript‑rendered listings).
- **Automatic page calculation** – specify total listings and the spider calculates required pages (24 listings per page).
- **Fetch date tracking** – captures when each URL and listing was fetched for data auditing.
- **Optimized requests** to speed up crawling and reduce bandwidth.
- **Two-stage scraping pipeline** – URL collection followed by detailed listing extraction.
- **Automated data cleaning** – extract properties, amenities, and normalize data formats.

---

## Requirements

- **Python 3.9+** (tested on 3.11)

## Installation

### With UV (Recommended)

```bash
# 1. Clone the repository
git clone https://github.com/kcnewman/jijiScrapper.git
cd jijiScrapper

# 2. Create a virtual environment with uv
uv venv

# 3. Activate the virtual environment
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# 4. Install dependencies with uv
uv sync

# 5. Install Playwright browsers
playwright install
```

### With Conda/Pip

```bash
# 1. Clone the repository
git clone https://github.com/kcnewman/jijiScrapper.git
cd jijiScrapper

# 2. Create a virtual environment
conda create env -n scraper
conda activate scraper

# 3. Install dependencies
pip3 install -r requirements.txt

# 4. Install Playwright browsers
playwright install
```

**Note:** If you encounter permission issues on Windows, run the terminal as Administrator.

---

## Usage

The easiest way to run the scraper is using the interactive CLI.

```bash
# If using uv
uv run main.py

# If using conda/pip
python main.py
```

### URL Spider

The **UrlSpider** collects listing URLs from search results pages.

**Input:**

- Base URL (default: Greater Accra rentals on Jiji.com.gh)
- **Total number of listings** on the site

The spider automatically calculates how many pages to scrape (20 listings per page).

**Output:**

- CSV file in `outputs/urls/` with columns: `url`, `page`, `fetch_date`

### Listing Spider

The **ListingSpider** extracts detailed information from each listing URL.

**Input:**

- CSV file with URLs from UrlSpider

**Extracted Data:**

- Title, location, price
- House type, bedrooms, bathrooms
- Properties and amenities
- Fetch date (when the listing was scraped)

**Output:**

- CSV file in `outputs/data/` with all extracted fields

### Data Cleaning (Housing Data Use Case)

Automatically processes scraped data to:

- Extract sub-location from location strings
- Fill missing house types with "Bedsitter"
- Normalize bedroom/bathroom data
- Expand properties into separate columns
- Create binary columns for amenities
- Preserve original fetch dates for data lineage

---

## Project Structure

```
jijiScrapper/
├── main.py                # Interactive CLI entry point
├── README.md              # This file
├── requirements.txt       # Python dependencies
├── scrapy.cfg             # Scrapy configuration
├── scrappers/             # Scrapy project
│   ├── spiders/
│   │   ├── urlspider.py   # Collects listing URLs
│   │   └── listingspider.py # Extracts listing details
│   ├── items.py
│   ├── pipelines.py
│   ├── middlewares.py
│   └── settings.py
├── scripts/
│   └── clean.py           # Data cleaning pipeline
├── notebooks/
│   └── eda.ipynb          # Exploratory data analysis
└── outputs/
    ├── urls/              # URL collection results
    └── data/              # Final cleaned listings
```

---

## Data Flow

1. **URL Collection** → UrlSpider captures URLs and fetch timestamps
2. **Listing Extraction** → ListingSpider retrieves details, preserves fetch dates
3. **Data Cleaning** → DataCleaner normalizes formats and expands fields
4. **Final Output** → Clean CSV with all listings and metadata
