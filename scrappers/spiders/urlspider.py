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
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            "headless": True,
        },
        "PLAYWRIGHT_ABORT_REQUEST": abortRequest,
        "DOWNLOAD_DELAY": 0.3,
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "DOWNLOAD_TIMEOUT": 60,
        "RETRY_TIMES": 2,
        "LOG_LEVEL": "INFO",
        "FEEDS": {
            os.path.join(SAVE_DIR, f"listingURLS_{timestamp}.csv"): {
                "format": "csv",
                "fields": ["url", "page"],
                "overwrite": True,
            }
        },
    }

    def __init__(self, baseUrl=None, startPage=None, maxPage=None, *args, **kwargs):
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

        if maxPage:
            self.maxPage = int(maxPage)
        else:
            self.maxPage = 1

        self.total_pages = self.maxPage - self.startPage + 1
        self.scraped_count = 0

        self.logger.info(f"Initialized with baseUrl: {self.baseUrl}")
        self.logger.info(f"Start page: {self.startPage}, Max page: {self.maxPage}")
        self.logger.info(f"Total pages to scrape: {self.total_pages}")

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
                yield {"url": absUrl, "page": currPage}

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
