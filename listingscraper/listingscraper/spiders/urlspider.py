import scrapy
from scrapy_playwright.page import PageMethod


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

    start_page = 1
    max_page = 2  # Maximum page number
    base_url = "https://jiji.com.gh/greater-accra/houses-apartments-for-rent?page={}"

    def start_requests(self):
        url = self.base_url.format(self.start_page)
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
            dont_filter=True,
        )

    async def parse(self, response):
        page = response.meta["playwright_page"]
        current_page = response.meta["current_page"]

        try:
            # Extract listings from current page
            links = response.css("div.b-advert-listing a::attr(href)").getall()
            self.logger.info(f"Page {current_page}: Found {len(links)} links")

            for href in links:
                absUrl = response.urljoin(href)
                yield {"url": absUrl, "page": current_page}

            # Move to next page if not at max_page
            if current_page < self.max_page:
                next_page = current_page + 1
                next_url = self.base_url.format(next_page)

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
                self.logger.info("Reached maximum page number")
                await page.close()

        except Exception as e:
            self.logger.error(f"Error on page {current_page}: {str(e)}")
            await page.close()

    async def errback_close_page(self, failure):
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()
