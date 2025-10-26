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

        if not base_url:
            raise ValueError("base_url is required")
        if not total_listings:
            raise ValueError("total_listings is required")

        self.base_url = base_url.rstrip(
            "/"
        )  # Ensure no trailing slash for consistent URL building
        self.total_listings = int(total_listings)
        self.listings_per_page = 20  # As per your observation
        self.start_page = 1
        self.max_page = ceil(self.total_listings / self.listings_per_page)
        self.processed_urls = set()  # To ensure no duplicate URLs are yielded

        self.logger.info(f"Initializing URL spider with:")
        self.logger.info(f"Base URL: {self.base_url}")
        self.logger.info(f"Total listings: {self.total_listings}")
        self.logger.info(f"Calculated max pages: {self.max_page}")

    def start_requests(self):
        url = f"{self.base_url}?page={self.start_page}"
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
                "current_page": self.start_page,
            },
            errback=self.errback_close_page,
            dont_filter=True,  # Allow retries for initial request
        )

    async def parse(self, response):
        page = response.meta["playwright_page"]
        curr_page = response.meta["current_page"]

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
                        "page": curr_page,
                    }

            self.logger.info(
                f"Page {curr_page}/{self.max_page}: Found {len(links)} total links, {new_links_on_page} new unique links."
            )
            self.logger.info(
                f"Total unique URLs collected so far: {len(self.processed_urls)}"
            )

            if curr_page < self.max_page:
                next_page = curr_page + 1
                next_url = f"{self.base_url}?page={next_page}"
                await page.close()  # Close current page before requesting next

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
                    f"Reached maximum page number ({self.max_page}). Total unique URLs: {len(self.processed_urls)}"
                )
                await page.close()  # Ensure page is closed when max_page is reached

        except Exception as e:
            self.logger.error(f"Error on page {curr_page} ({response.url}): {str(e)}")
            if page:
                await page.close()

    async def errback_close_page(self, failure):
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()
        self.logger.error(f"Request failed for {failure.request.url}: {failure.value}")
