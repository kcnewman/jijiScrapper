# Jiji Scrapper

A lightweight, **Scrapy + Playwright** based web-scraper for extracting listings from the popular **Jiji.com.gh** site.
The project provides two spiders:

1.  **UrlSpider** – crawls paginated search results and collects the URLs of individual listings.
2.  **ListingSpider** – visits each URL and extracts detailed listing information (title, price, location, specs, amenities, etc.).

---

## Features

- **Dynamic page handling** with `scrapy-playwright` (JavaScript‑rendered listings).
- **Robust pagination** and automatic page‑range selection.
- **Optimized requests** to speed up crawling and reduce bandwidth.

---

## Requirements

- **Python 3.9+** (tested on 3.11)

## Installation

```powershell
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

```python
python main.py
```
