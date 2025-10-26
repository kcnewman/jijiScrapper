import scrapy


class ListingspiderSpider(scrapy.Spider):
    name = "listingspider"
    allowed_domains = ["s"]
    start_urls = ["https://s"]

    def parse(self, response):
        pass
