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

# Fix for asyncio event loop errors
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
        "PLAYWRIGHT_MAX_CONTEXTS": 10,  # Increased from 8
        "PLAYWRIGHT_MAX_PAGES_PER_CONTEXT": 5,  # Increased from 4
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            "headless": True,
            "args": [
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-setuid-sandbox",
            ],
        },
        "PLAYWRIGHT_CONTEXTS": {
            "default": {
                "ignore_https_errors": True,
                "bypass_csp": True,
                "java_script_enabled": True,
                "accept_downloads": False,
            }
        },
        "PLAYWRIGHT_ABORT_REQUEST": lambda req: req.resource_type
        in ["image", "media", "font", "stylesheet", "other"],
        "DOWNLOAD_DELAY": 0.05,  # Reduced from 0.1
        "CONCURRENT_REQUESTS": 20,  # Increased from 16
        "CONCURRENT_REQUESTS_PER_DOMAIN": 15,  # Increased from 12
        "DOWNLOAD_TIMEOUT": 25,  # Reduced from 30
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 25000,  # Reduced from 30000
        # AutoThrottle for smart rate limiting
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0.1,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 10.0,
        "RETRY_TIMES": 1,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 408, 429],
        "COOKIES_ENABLED": False,
        "TELNETCONSOLE_ENABLED": False,
        "LOG_LEVEL": "INFO",
        "FEEDS": {
            os.path.join(
                SAVE_DIR, f"listings__{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            ): {
                "format": "csv",
            },
        },
    }

    def __init__(self, csv_path="outputs/urls/combined_urls.csv", *args, **kwargs):
        super(ListingSpider, self).__init__(*args, **kwargs)
        if not os.path.isabs(csv_path):
            self.csv_path = os.path.join(PROJECT_ROOT, csv_path)
        else:
            self.csv_path = csv_path
        self.urls = self.load_urls()
        self.total_listings = len(self.urls)
        self.scraped_count = 0
        self.start_time = datetime.now()  # Added for speed tracking

    def load_urls(self):
        """Load URLs and fetch_date from CSV file"""
        urls = []
        try:
            with open(self.csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row and row.get("url", "").strip():
                        urls.append(
                            {
                                "url": row["url"].strip(),
                                "fetch_date": row.get("fetch_date", None),
                            }
                        )
        except FileNotFoundError:
            self.logger.error(f"CSV file not found: {self.csv_path}")
        return urls

    def start_requests(self):
        """Generate requests for all URLs"""
        for url_data in self.urls:
            yield scrapy.Request(
                url_data["url"],
                meta={
                    "playwright": True,
                    "playwright_context": "default",
                    "playwright_page_goto_kwargs": {
                        "wait_until": "domcontentloaded",
                        "timeout": 20000,  # Reduced from 30000
                    },
                    "playwright_page_methods": [
                        # Reduced timeout
                        PageMethod(
                            "wait_for_selector", "h1", timeout=8000
                        ),  # Reduced from 10000
                    ],
                    "playwright_include_page": True,
                    "fetch_date": url_data.get("fetch_date"),
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

            # Amenities extraction - optimized with shorter timeout
            amenities = []
            if page:
                try:
                    amenities_section = await asyncio.wait_for(
                        page.query_selector(".b-advert-attributes--tags"),
                        timeout=1.5,  # Reduced from 2.0
                    )

                    if amenities_section:
                        amenity_elements = await page.query_selector_all(
                            ".b-advert-attributes__tag"
                        )

                        amenity_tasks = [
                            element.text_content() for element in amenity_elements
                        ]
                        amenity_texts = await asyncio.gather(
                            *amenity_tasks, return_exceptions=True
                        )

                        for text in amenity_texts:
                            if isinstance(text, str) and text:
                                amenity_text = text.strip()
                                if amenity_text and amenity_text not in amenities:
                                    amenities.append(amenity_text)

                except asyncio.TimeoutError:
                    self.logger.debug(f"Amenities section timeout for {response.url}")
                except Exception as e:
                    self.logger.debug(f"Error extracting amenities: {e}")

            # Extract price
            price = response.css(
                ".b-alt-advert-price-wrapper span.qa-advert-price-view-value::text, "
                ".b-alt-advert-price-wrapper .qa-advert-price::text, "
                ".b-alt-advert-price-wrapper div::text"
            ).get()

            # Extract description
            description = response.css(".qa-description-text::text").get()

            self.scraped_count += 1

            # Log every 10 items with speed tracking
            if self.scraped_count % 10 == 0:
                elapsed = (datetime.now() - self.start_time).total_seconds()
                lpm = (self.scraped_count / elapsed) * 60 if elapsed > 0 else 0
                self.logger.info(
                    f"Scraped {self.scraped_count}/{self.total_listings} ({lpm:.0f} listings/min)"
                )

            yield {
                "url": response.url,
                "description": description.strip() if description else None,
                "fetch_date": response.meta.get("fetch_date"),
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

    def closed(self, reason):
        """Log final stats"""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        avg_lpm = (self.scraped_count / elapsed) * 60 if elapsed > 0 else 0

        self.logger.info(f"\n{'=' * 60}")
        self.logger.info(f"Spider closed: {reason}")
        self.logger.info(f"Total scraped: {self.scraped_count}/{self.total_listings}")
        self.logger.info(f"Time taken: {elapsed / 60:.1f} minutes")
        self.logger.info(f"Average speed: {avg_lpm:.0f} listings/min")
        self.logger.info(f"{'=' * 60}\n")
