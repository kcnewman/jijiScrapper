import os
import pathlib
from datetime import datetime
import scrapy
from scrapy_playwright.page import PageMethod

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SAVE_DIR = PROJECT_ROOT / "outputs" / "urls"
SAVE_DIR.mkdir(parents=True, exist_ok=True)


def abortRequest(request):
    if request.resource_type == "image":
        return True
    return False


class UrlSpider(scrapy.Spider):
    name = "urlspider"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    custom_settings = {
        "PLAYWRIGHT_ABORT_REQUEST": abortRequest,
        "DOWNLOAD_TIMEOUT": 60,
        "RETRY_TIMES": 3,
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

        # Set base URL with default
        if baseUrl:
            self.baseUrl = baseUrl
        else:
            self.baseUrl = (
                "https://jiji.com.gh/greater-accra/houses-apartments-for-rent?page={}"
            )

        # Set start page with default
        if startPage:
            self.startPage = int(startPage)
        else:
            self.startPage = 1

        # Set max page with default
        if maxPage:
            self.maxPage = int(maxPage)
        else:
            self.maxPage = 1

        self.logger.info(f"Initialized with baseUrl: {self.baseUrl}")
        self.logger.info(f"Start page: {self.startPage}, Max page: {self.maxPage}")

    def start_requests(self):
        url = self.baseUrl.format(self.startPage)
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
                "current_page": self.startPage,
            },
            errback=self.errback_close_page,
            dont_filter=True,
        )

    async def parse(self, response):
        page = response.meta["playwright_page"]
        currPage = response.meta["current_page"]

        try:
            # Extract listings from current page
            links = response.css("div.b-advert-listing a::attr(href)").getall()
            self.logger.info(f"Page {currPage}: Found {len(links)} links")

            for href in links:
                absUrl = response.urljoin(href)
                yield {"url": absUrl, "page": currPage}

            # Move to next page if not at max_page
            if currPage < self.maxPage:
                nextPage = currPage + 1
                nextUrl = self.baseUrl.format(nextPage)

                await page.close()

                yield scrapy.Request(
                    url=nextUrl,
                    meta={
                        "playwright": True,
                        "playwright_page_methods": [
                            PageMethod(
                                "wait_for_selector",
                                "div.b-advert-listing",
                                timeout=30000,
                            )
                        ],
                        "playwright_include_page": True,
                        "current_page": nextPage,
                    },
                    callback=self.parse,
                    errback=self.errback_close_page,
                    dont_filter=True,
                )
            else:
                self.logger.info("Reached maximum page number")
                await page.close()

        except Exception as e:
            self.logger.error(f"Error on page {currPage}: {str(e)}")
            await page.close()

    async def errback_close_page(self, failure):
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()
