import os
import pathlib
from datetime import datetime
import scrapy
from scrapy_playwright.page import PageMethod
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
SAVE_DIR = PROJECT_ROOT / "outputs" / "urls"
SAVE_DIR.mkdir(parents=True, exist_ok=True)


def abortRequest(request):
    """Block unnecessary resources for faster loading"""
    return request.resource_type in ["image", "media", "font", "stylesheet"]


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
        "DOWNLOAD_DELAY": 0.1,  # Reduced from 0.3
        "CONCURRENT_REQUESTS": 16,  # Increased from 12
        "CONCURRENT_REQUESTS_PER_DOMAIN": 12,  # Increased from 8
        "DOWNLOAD_TIMEOUT": 30,  # Reduced from 60
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 30000,  # 30 seconds
        # AutoThrottle for smart rate limiting
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0.1,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 10.0,
        "RETRY_TIMES": 1,  # Reduced from 2
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 408, 429],
        "COOKIES_ENABLED": False,
        "TELNETCONSOLE_ENABLED": False,
        "LOG_LEVEL": "INFO",
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

        if baseUrl:
            self.baseUrl = baseUrl
        else:
            self.baseUrl = (
                "https://jiji.com.gh/greater-accra/houses-apartments-for-rent?page={}"
            )

        if startPage:
            self.startPage = int(startPage)
        else:
            self.startPage = 1

        # Calculate max page from total listings (24 listings per page)
        if totalListings:
            import math

            self.totalListings = int(totalListings)
            self.maxPage = self.startPage + math.ceil(self.totalListings / 20) - 1
        else:
            self.totalListings = 20
            self.maxPage = self.startPage

        self.total_pages = self.maxPage - self.startPage + 1
        self.scraped_count = 0

        self.logger.info(f"Initialized with baseUrl: {self.baseUrl}")
        self.logger.info(f"Start page: {self.startPage}, Max page: {self.maxPage}")
        self.logger.info(
            f"Total listings: {self.totalListings}, Total pages to scrape: {self.total_pages}"
        )

    def start_requests(self):
        """Generate all page requests upfront for parallel processing"""
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
        page = response.meta["playwright_page"]
        currPage = response.meta["current_page"]

        try:
            links = response.css("div.b-advert-listing a::attr(href)").getall()

            for href in links:
                absUrl = response.urljoin(href)
                yield {
                    "url": absUrl,
                    "page": currPage,
                    "fetch_date": datetime.now().isoformat(),
                }

            self.scraped_count += 1
            self.logger.info(
                f"Page {currPage}: Found {len(links)} links ({self.scraped_count}/{self.total_pages} pages completed)"
            )

        except Exception as e:
            self.logger.error(f"Error on page {currPage}: {str(e)}")

        finally:
            if page:
                await page.close()

    async def errback_close_page(self, failure):
        """Handle request failures and close page"""
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()
        self.logger.error(f"Error on {failure.request.url}: {failure.value}")
