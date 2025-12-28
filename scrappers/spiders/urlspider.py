import os
import pathlib
from datetime import datetime
import scrapy
from scrapy_playwright.page import PageMethod
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
logging.getLogger("scrapy").setLevel(logging.ERROR)
logging.getLogger("playwright").setLevel(logging.ERROR)

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SAVE_DIR = PROJECT_ROOT / "outputs" / "urls"
SAVE_DIR.mkdir(parents=True, exist_ok=True)


class UrlSpider(scrapy.Spider):
    name = "urlspider"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    custom_settings = {
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_MAX_CONTEXTS": 8,
        "PLAYWRIGHT_MAX_PAGES_PER_CONTEXT": 4,
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
        "DOWNLOAD_DELAY": 0.1,
        "CONCURRENT_REQUESTS": 16,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 12,
        "DOWNLOAD_TIMEOUT": 30,
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 30000,
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
            os.path.join(SAVE_DIR, f"listingURLS_{timestamp}.csv"): {
                "format": "csv",
                "fields": ["url", "page", "fetch_date"],
                "overwrite": True,
            }
        },
    }

    def __init__(
        self, baseUrl=None, startPage=None, totalListings=None, *args, **kwargs
    ):
        super(UrlSpider, self).__init__(*args, **kwargs)
        self.baseUrl = (
            baseUrl
            or "https://jiji.com.gh/greater-accra/houses-apartments-for-rent?page={}"
        )
        self.startPage = int(startPage) if startPage else 1

        import math

        self.totalListings = int(totalListings) if totalListings else 20
        # Jiji usually has ~20 items per page
        self.maxPage = self.startPage + math.ceil(self.totalListings / 20) - 1

        # UI Tracking Stats
        self.total_pages_to_visit = self.maxPage - self.startPage + 1
        self.pages_visited = 0
        self.successful_scrapes = 0
        self.failures = 0
        self.start_time = time.time()

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UrlSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_opened(self, spider):
        print("\n🚀 Starting scraping...")

    def spider_closed(self, spider):
        self.update_progress(force=True)  # Final UI update
        duration = time.time() - self.start_time
        print(f"\n\n{'=' * 40}")
        print("🏁 DONE")
        print(f"📑 Pages Visited: {self.pages_visited}")
        print(f"✅ Successes:     {self.successful_scrapes} URLs")
        print(f"❌ Failures:      {self.failures}")
        print(f"⏱️  Time taken:   {duration:.2f}s")
        print(f"{'=' * 40}\n")

    def start_requests(self):
        for page_num in range(self.startPage, self.maxPage + 1):
            url = self.baseUrl.format(page_num)
            yield scrapy.Request(
                url=url,
                meta={
                    "playwright": True,
                    "playwright_page_methods": [
                        PageMethod(
                            "wait_for_selector", "div.b-advert-listing", timeout=30000
                        )
                    ],
                    "playwright_include_page": True,
                    "current_page": page_num,
                },
                callback=self.parse,
                errback=self.errback_close_page,
                dont_filter=True,
            )

    async def parse(self, response):
        page = response.meta.get("playwright_page")
        currPage = response.meta["current_page"]

        try:
            links = response.css("div.b-advert-listing a::attr(href)").getall()
            for href in links:
                self.successful_scrapes += 1

                # Update UI only every 200 found URLs to prevent terminal lag
                if self.successful_scrapes % 200 == 0:
                    self.update_progress()

                yield {
                    "url": response.urljoin(href),
                    "page": currPage,
                    "fetch_date": datetime.now().strftime("%Y-%m-%d"),
                }

            self.pages_visited += 1

        except Exception:
            self.failures += 1
        finally:
            if page:
                await page.close()

    def update_progress(self, force=False):
        elapsed = time.time() - self.start_time
        # items/min = (items / seconds) * 60
        items_min = (self.successful_scrapes / elapsed) * 60 if elapsed > 0 else 0

        # \r at start keeps it on one line. Spaces at end clear old characters.
        sys.stdout.write(
            f"\r⏳ Progress: [{self.pages_visited}/{self.total_pages_to_visit}] Pages "
            f"| 🔗 {self.successful_scrapes} URLs "
            f"| ⚡ {items_min:.0f} items/min   "
        )
        sys.stdout.flush()

    async def errback_close_page(self, failure):
        self.failures += 1
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()
