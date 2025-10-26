import argparse
import pathlib
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from listingscraper.spiders.urlspider import urlspiderSpider
from listingscraper.spiders.listingspider import ListingspiderSpider
from datetime import datetime
import os

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent
URLS_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "urls"
LISTING_DETAILS_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "data"

URLS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LISTING_DETAILS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def run_spider(spider_cls, settings, **kwargs):
    """Helper to run a single spider."""
    process = CrawlerProcess(settings)
    process.crawl(spider_cls, **kwargs)
    process.start()


def main():
    parser = argparse.ArgumentParser(description="Jiji Scraper")
    parser.add_argument(
        "--url",
        required=True,
        help="Base URL of the category to scrape (Housing, Electronics etc.)",
    )
    parser.add_argument(
        "--total",
        required=True,
        type=int,
        help="Total number of listings to consider",
    )

    args = parser.parse_args()

    print(f"Starting Jiji Scraper with URL: {args.url}")
    print(f"Total listings to process: {args.total}")

    # Executing URL spider
    print("\n--- Running URL Spider (Stage 1/2) ---")

    # Savr the file with the time at run
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    urls_output_filename = f"listingURLS_{timestamp}.csv"
    urls_output_filepath = URLS_OUTPUT_DIR / urls_output_filename

    url_spider_settings = get_project_settings()
    url_spider_settings.set(
        "FEEDS",
        {
            str(urls_output_filepath): {
                "format": "csv",
                "fields": ["url", "page"],
                "overwrite": True,
            }
        },
    )

    run_spider(
        urlspiderSpider,
        url_spider_settings,
        base_url=args.url,
        total_listings=args.total,
    )

    if not urls_output_filepath.exists() or urls_output_filepath.stat().st_size == 0:
        print(
            f"Error: No URLs collected or file is empty at {urls_output_filepath}. Exiting."
        )
        return

    print(f"URLs collected and saved to: {urls_output_filepath}")

    # Listing spider execution
    print("\n--- Running Listing Details Spider (Stage 2/2) ---")

    listing_details_output_filename = f"listing_details_{timestamp}.csv"
    listing_details_output_filepath = (
        LISTING_DETAILS_OUTPUT_DIR / listing_details_output_filename
    )

    listing_spider_settings = get_project_settings()
    listing_spider_settings.set(
        "FEEDS",
        {
            str(listing_details_output_filepath): {
                "format": "csv",
                "fields": [
                    "url",
                    "title",
                    "location",
                    "house_type",
                    "num_bathrooms",
                    "num_bedrooms",
                    "price",
                    "properties",
                    "amenities",
                ],
                "overwrite": True,
            }
        },
    )

    run_spider(
        ListingspiderSpider,
        listing_spider_settings,
        urls_file=str(urls_output_filepath),
    )

    print(f"Listing details collected and saved to: {listing_details_output_filepath}")
    print("\n--- Scraping process completed ---")


if __name__ == "__main__":
    main()
