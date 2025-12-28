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
import time
from scrapy import signals

# Fix for asyncio event loop errors
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

warnings.filterwarnings("ignore", category=RuntimeWarning, module="asyncio")
logging.getLogger("asyncio").setLevel(logging.CRITICAL)
# Silencing Scrapy noise to keep the UI clean
logging.getLogger("scrapy").setLevel(logging.ERROR)
logging.getLogger("playwright").setLevel(logging.ERROR)

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SAVE_DIR = PROJECT_ROOT / "outputs" / "data"
SAVE_DIR.mkdir(parents=True, exist_ok=True)


class ListingSpider(scrapy.Spider):
    name = "listingspider"

    # PRESERVED YOUR EXACT SETTINGS
    custom_settings = {
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_MAX_CONTEXTS": 10,
        "PLAYWRIGHT_MAX_PAGES_PER_CONTEXT": 5,
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
        "DOWNLOAD_DELAY": 0.05,
        "CONCURRENT_REQUESTS": 20,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 15,
        "DOWNLOAD_TIMEOUT": 25,
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 25000,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0.1,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 10.0,
        "RETRY_TIMES": 1,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 408, 429],
        "COOKIES_ENABLED": False,
        "TELNETCONSOLE_ENABLED": False,
        "LOG_ENABLED": False,
        "LOG_LEVEL": "ERROR",
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

        # UI Tracking Stats
        self.scraped_count = 0
        self.failures = 0
        self.start_time_ts = time.time()

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(ListingSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_opened(self, spider):
        sys.stdout.write("\n🚀 Starting data scrape...\n")
        sys.stdout.flush()

    def spider_closed(self, spider, reason):
        self.update_progress(force=True)
        duration = time.time() - self.start_time_ts
        print(f"\n\n{'=' * 40}")
        print("🏁 DONE")
        print(f"✅ Successful: {self.scraped_count}/{self.total_listings}")
        print(f"❌ Failures:   {self.failures}")
        print(f"⏱️  Duration:   {duration / 60:.1f} minutes")
        print(f"{'=' * 40}\n")

    def load_urls(self):
        urls = []
        today_str = datetime.now().strftime("%Y-%m-%d")
        try:
            with open(self.csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    url = row.get("url", "").strip()
                    if url:
                        f_date = row.get("fetch_date")
                        if not f_date or str(f_date).strip().lower() == "nan":
                            f_date = today_str

                        urls.append(
                            {
                                "url": row["url"].strip(),
                                "fetch_date": f_date,
                            }
                        )
        except FileNotFoundError:
            pass  # main.py handles error logging for missing files
        return urls

    def start_requests(self):
        for url_data in self.urls:
            yield scrapy.Request(
                url_data["url"],
                meta={
                    "playwright": True,
                    "playwright_context": "default",
                    "playwright_page_goto_kwargs": {
                        "wait_until": "domcontentloaded",
                        "timeout": 20000,
                    },
                    "playwright_page_methods": [
                        PageMethod("wait_for_selector", "h1", timeout=8000),
                    ],
                    "playwright_include_page": True,
                    "fetch_date": url_data.get("fetch_date"),
                },
                callback=self.parse,
                errback=self.errback_close_page,
            )

    async def parse(self, response):
        page = response.meta.get("playwright_page")

        try:
            title = response.css("h1 div::text, .b-advert-title-outer h1::text").get()
            location = response.css(".b-advert-info-statistics--region::text").get()

            properties = {}
            for prop in response.css(".b-advert-attribute"):
                key = prop.css(".b-advert-attribute__key::text").get()
                value = prop.css(".b-advert-attribute__value::text").get()
                if key and value:
                    properties[key.strip().rstrip(":")] = value.strip()

            house_type, bathrooms, bedrooms = None, None, None
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

            amenities = []
            if page:
                try:
                    amenities_section = await asyncio.wait_for(
                        page.query_selector(".b-advert-attributes--tags"),
                        timeout=1.5,
                    )
                    if amenities_section:
                        amenity_elements = await page.query_selector_all(
                            ".b-advert-attributes__tag"
                        )
                        amenity_tasks = [el.text_content() for el in amenity_elements]
                        amenity_texts = await asyncio.gather(
                            *amenity_tasks, return_exceptions=True
                        )
                        for text in amenity_texts:
                            if isinstance(text, str) and text.strip():
                                amenity_text = text.strip()
                                if amenity_text not in amenities:
                                    amenities.append(amenity_text)
                except Exception:
                    pass

            price = response.css(
                ".b-alt-advert-price-wrapper span.qa-advert-price-view-value::text, "
                ".b-alt-advert-price-wrapper .qa-advert-price::text, "
                ".b-alt-advert-price-wrapper div::text"
            ).get()

            description = response.css(".qa-description-text::text").get()

            self.scraped_count += 1
            if self.scraped_count % 10 == 0:
                self.update_progress()

            yield {
                "url": response.url,
                "description": description.strip() if description else None,
                "fetch_date": response.meta.get("fetch_date")
                or datetime.now().strftime("%Y-%m-%d"),
                "title": title.strip() if title else None,
                "location": location.strip() if location else None,
                "house_type": house_type,
                "bathrooms": bathrooms,
                "bedrooms": bedrooms,
                "properties": properties,
                "amenities": amenities,
                "price": price.strip() if price else None,
            }

        except Exception:
            self.failures += 1
        finally:
            if page:
                await page.close()

    def update_progress(self, force=False):
        elapsed = time.time() - self.start_time_ts
        lpm = (self.scraped_count / elapsed) * 60 if elapsed > 0 else 0

        sys.stdout.write(
            f"\r⏳ Progress: [{self.scraped_count}/{self.total_listings}] "
            f"| ⚡ Speed: {lpm:.0f} items/min          "
        )
        sys.stdout.flush()

    async def errback_close_page(self, failure):
        self.failures += 1
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()
