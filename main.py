import argparse
import pathlib
import subprocess
from datetime import datetime
import os
import sys  # Import sys to get the Python executable path

# Define project root and output directories
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent
URLS_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "urls"
LISTING_DETAILS_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "data"

# Ensure output directories exist
URLS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LISTING_DETAILS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def main():
    parser = argparse.ArgumentParser(description="Jiji Scraper")
    parser.add_argument(
        "--url",
        required=True,
        help="Base URL of the category to scrape (e.g., https://jiji.com.gh/greater-accra/houses-apartments-for-rent)",
    )
    parser.add_argument(
        "--total",
        required=True,
        type=int,
        help="Total number of listings to consider for pagination calculation (e.g., 16600 for 830 pages * 20 listings/page)",
    )

    args = parser.parse_args()

    print(f"Starting Jiji Scraper with URL: {args.url}")
    print(f"Total listings to process: {args.total}")

    # --- Step 1: Run urlspider to collect listing URLs ---
    print("\n--- Running URL Spider (Stage 1/2) ---")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    urls_output_filename = f"listingURLS_{timestamp}.csv"
    urls_output_filepath = URLS_OUTPUT_DIR / urls_output_filename

    # Construct the Scrapy command for urlspider
    # We use sys.executable to ensure the correct python environment's scrapy is used
    # and -m scrapy to run scrapy as a module.
    url_spider_command = [
        sys.executable,
        "-m",
        "scrapy",
        "crawl",
        "urlspider",
        "-a",
        f"base_url={args.url}",
        "-a",
        f"total_listings={args.total}",
        "-o",
        str(urls_output_filepath),  # Output to CSV
        "-t",
        "csv",  # Specify output format
    ]

    print(f"Executing command: {' '.join(url_spider_command)}")
    result = subprocess.run(
        url_spider_command,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"Error running urlspider:\n{result.stderr}")
        return
    else:
        print(f"urlspider output:\n{result.stdout}")

    if not urls_output_filepath.exists() or urls_output_filepath.stat().st_size == 0:
        print(
            f"Error: No URLs collected or file is empty at {urls_output_filepath}. Exiting."
        )
        return

    print(f"URLs collected and saved to: {urls_output_filepath}")

    # --- Step 2: Run listingspider to collect detailed information ---
    print("\n--- Running Listing Details Spider (Stage 2/2) ---")

    listing_details_output_filename = f"listing_details_{timestamp}.csv"
    listing_details_output_filepath = (
        LISTING_DETAILS_OUTPUT_DIR / listing_details_output_filename
    )

    # Construct the Scrapy command for listingspider
    listing_spider_command = [
        sys.executable,
        "-m",
        "scrapy",
        "crawl",
        "listingspider",
        "-a",
        f"urls_file={urls_output_filepath}",  # Pass the path to the collected URLs
        "-o",
        str(listing_details_output_filepath),  # Output to CSV
        "-t",
        "csv",  # Specify output format
    ]

    print(f"Executing command: {' '.join(listing_spider_command)}")
    result = subprocess.run(
        listing_spider_command,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"Error running listingspider:\n{result.stderr}")
        return
    else:
        print(f"listingspider output:\n{result.stdout}")

    print(f"Listing details collected and saved to: {listing_details_output_filepath}")
    print("\n--- Scraping process completed ---")


if __name__ == "__main__":
    main()
