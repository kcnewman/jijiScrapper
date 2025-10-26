from collections.abc import Iterable
from typing import Any
import scrapy


class ListingspiderSpider(scrapy.Spider):
    name = "listingspider"

    def start_requests(self):
        urlPath = urlPath
        for url in urlPath:
            yield scrapy.Request(url)

    def parse(self, response):
        pass
