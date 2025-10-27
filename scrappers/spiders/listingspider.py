import os
import pathlib
import scrapy
import csv
from scrapy_playwright.page import PageMethod
from datetime import datetime
import asyncio
import sys
import warnings
import logging

# Fix for asyncio evemt loop errors
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

warnings.filterwarnings("ignore", category=RuntimeWarning, module="asyncio")
logging.getLogger("asyncio").setLevel(logging.CRITICAL)

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SAVE_DIR = PROJECT_ROOT / "outputs" / "data"
SAVE_DIR.mkdir(parents=True, exist_ok=True)


class ListingSpider(scrapy.Spider):
    name = "listingspider"

    custom_settings = {
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            "headless": True,
        },
        "PLAYWRIGHT_ABORT_REQUEST": lambda req: req.resource_type
        in ["image", "media", "font", "stylesheet"],
        "DOWNLOAD_DELAY": 0.3,
        "CONCURRENT_REQUESTS": 12,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "DOWNLOAD_TIMEOUT": 60,
        "RETRY_TIMES": 2,
        "LOG_LEVEL": "INFO",  # suppress asyncio warnings
        "FEEDS": {
            os.path.join(
                SAVE_DIR, f"listings__{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            ): {
                "format": "csv",
            },
        },
    }

    def __init__(
        self, csv_path="outputs/urls/listingURLS_20251026_165941.csv", *args, **kwargs
    ):
        super(ListingSpider, self).__init__(*args, **kwargs)
        if not os.path.isabs(csv_path):
            self.csv_path = os.path.join(PROJECT_ROOT, csv_path)
        else:
            self.csv_path = csv_path
        self.urls = self.load_urls()
        self.total_listings = len(self.urls)
        self.scraped_count = 0

    def load_urls(self):
        """Load URLs from CSV file"""
        urls = []
        try:
            with open(self.csv_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                next(reader)  # this line skips header row
                for row in reader:
                    if row and row[0].strip():
                        urls.append(row[0].strip())
        except FileNotFoundError:
            self.logger.error(f"CSV file not found: {self.csv_path}")
        return urls

    def start_requests(self):
        """Generate requests for all URLs"""
        for url in self.urls:
            yield scrapy.Request(
                url,
                meta={
                    "playwright": True,
                    "playwright_page_methods": [
                        # avoid full loading of page
                        PageMethod("wait_for_selector", "h1", timeout=15000),
                    ],
                    "playwright_include_page": True,
                },
                callback=self.parse,
                errback=self.errback_close_page,
            )

    async def parse(self, response):
        """Extract listing information using Playwright page for amenities"""
        page = response.meta.get("playwright_page")

        try:
            # Extract title, location
            title = response.css("h1 div::text, .b-advert-title-outer h1::text").get()
            location = response.css(".b-advert-info-statistics--region::text").get()

            # Extract all key-value properties
            properties = {}
            for prop in response.css(".b-advert-attribute"):
                key = prop.css(".b-advert-attribute__key::text").get()
                value = prop.css(".b-advert-attribute__value::text").get()
                if key and value:
                    properties[key.strip().rstrip(":")] = value.strip()

            # Extract house details
            house_type = None
            bathrooms = None
            bedrooms = None

            icon_details_texts = response.css(
                ".b-advert-icon-attribute span::text, .b-advert-icon-attribute__value::text"
            ).getall()

            for detail in icon_details_texts:
                detail_lower = detail.lower()
                if "bed" in detail_lower:
                    bedrooms = detail.strip()
                elif "bath" in detail_lower:
                    bathrooms = detail.strip()
                elif not house_type:
                    house_type = detail.strip()

            if not house_type:
                house_type = properties.get("Subtype") or properties.get("Type")
            if not bedrooms:
                bedrooms = properties.get("Bedrooms")
            if not bathrooms:
                bathrooms = properties.get("Bathrooms") or properties.get("Toilets")

            # ameneties extraction
            amenities = []
            if page:
                try:
                    # don't wait if ameneties don't exit
                    amenities_section = await page.query_selector(
                        ".b-advert-attributes--tags"
                    )

                    if amenities_section:
                        amenity_elements = await page.query_selector_all(
                            ".b-advert-attributes__tag"
                        )

                        for element in amenity_elements:
                            text = await element.text_content()
                            if text:
                                amenity_text = text.strip()
                                if amenity_text and amenity_text not in amenities:
                                    amenities.append(amenity_text)

                except Exception as e:
                    pass

            # Extract price
            price = response.css(
                ".b-alt-advert-price-wrapper span.qa-advert-price-view-value::text, "
                ".b-alt-advert-price-wrapper .qa-advert-price::text, "
                ".b-alt-advert-price-wrapper div::text"
            ).get()

            self.scraped_count += 1
            self.logger.info(
                f"Scraped listing {self.scraped_count}/{self.total_listings}"
            )

            yield {
                "url": response.url,
                "title": title.strip() if title else None,
                "location": location.strip() if location else None,
                "house_type": house_type,
                "bathrooms": bathrooms,
                "bedrooms": bedrooms,
                "properties": properties,
                "amenities": amenities,
                "price": price.strip() if price else None,
            }

        except Exception as e:
            self.logger.error(f"Error parsing {response.url}: {e}", exc_info=True)

        finally:
            # Always close the page to save memory
            if page:
                try:
                    await page.close()
                except Exception as e:
                    self.logger.warning(f"Error closing page: {e}")

    async def errback_close_page(self, failure):
        """Handle request failures and close page"""
        page = failure.request.meta.get("playwright_page")
        if page:
            try:
                await page.close()
            except Exception as e:
                self.logger.warning(f"Error closing page in errback: {e}")
        self.logger.error(f"Error on {failure.request.url}: {failure.value}")
