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

    def start_requests(self):
        yield scrapy.Request(
            url="https://jiji.com.gh/greater-accra/houses-apartments-for-rent",
            meta={
                "playwright": True,
                "playwright_page_methods": [
                    PageMethod(
                        "wait_for_selector", "div.b-advert-listing", timeout=30000
                    )
                ],
                "playwright_include_page": True,
            },
            errback=self.errback_close_page,
        )

    async def parse(self, response):
        page = response.meta["playwright_page"]
        try:
            links = response.css("div.b-advert-listing a::attr(href)").getall()
            self.logger.info(f"Found {len(links)} links")

            for href in links:
                absUrl = response.urljoin(href)
                yield {"url": absUrl}
        finally:
            await page.close()

    async def errback_close_page(self, failure):
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()
