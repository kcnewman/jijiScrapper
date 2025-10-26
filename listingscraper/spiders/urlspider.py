import scrapy
from scrapy_playwright.page import PageMethod
from math import ceil


def abortRequest(request):
    if request.resource_type == "image":
        return True
    return False


class urlspiderSpider(scrapy.Spider):
    name = "urlspider"

    custom_settings = {
        "PLAYWRIGHT_ABORT_REQUEST": abortRequest,
        "DOWNLOAD_TIMEOUT": 60,
        "RETRY_TIMES": 3,
    }

    def __init__(self, baseURL=None, totalListing=None, *args, **kwargs):
        super(urlspiderSpider, self).__init__(*args, **kwargs)

        if not baseURL:
            raise ValueError("base_url is required")
        if not totalListing:
            raise ValueError("total_listings is required")

        self.base_url = baseURL.rstrip(
            "/"
        )  # Ensure no trailing slash for consistent URL building
        self.totalListing = int(totalListing)
        self.listingPPage = 20  # As per your observation
        self.startPage = 1
        self.maxPage = ceil(self.totalListing / self.listingPPage)
        self.processed_urls = set()  # To ensure no duplicate URLs are yielded

        self.logger.info("Initializing URL spider with:")
        self.logger.info(f"Base URL: {self.base_url}")
        self.logger.info(f"Total listings: {self.totalListing}")
        self.logger.info(f"Calculated max pages: {self.maxPage}")

    def start_requests(self):
        url = f"{self.base_url}?page={self.startPage}"
        self.logger.info(f"Starting URL requests from: {url}")
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
            dont_filter=True,  # Allow retries for initial request
        )

    async def parse(self, response):
        page = response.meta["playwright_page"]
        currPage = response.meta["current_page"]

        try:
            links = response.css("div.b-advert-listing a::attr(href)").getall()
            new_links_on_page = 0

            for href in links:
                abs_url = response.urljoin(href)
                if abs_url not in self.processed_urls:
                    self.processed_urls.add(abs_url)
                    new_links_on_page += 1
                    yield {
                        "url": abs_url,
                        "page": currPage,
                    }

            self.logger.info(
                f"Page {currPage}/{self.maxPage}: Found {len(links)} total links, {new_links_on_page} new unique links."
            )
            self.logger.info(
                f"Total unique URLs collected so far: {len(self.processed_urls)}"
            )

            if currPage < self.maxPage:
                next_page = currPage + 1
                next_url = f"{self.base_url}?page={next_page}"
                await page.close()

                yield scrapy.Request(
                    url=next_url,
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
                        "current_page": next_page,
                    },
                    callback=self.parse,
                    errback=self.errback_close_page,
                    dont_filter=True,
                )
            else:
                self.logger.info(
                    f"Reached maximum page number ({self.maxPage}). Total unique URLs: {len(self.processed_urls)}"
                )
                await page.close()  # Ensure page is closed when max_page is reached

        except Exception as e:
            self.logger.error(f"Error on page {currPage} ({response.url}): {str(e)}")
            if page:
                await page.close()

    async def errback_close_page(self, failure):
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()
        self.logger.error(f"Request failed for {failure.request.url}: {failure.value}")
