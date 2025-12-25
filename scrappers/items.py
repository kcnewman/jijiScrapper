# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ListingItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    location = scrapy.Field()
    house_type = scrapy.Field()
    num_bathrooms = scrapy.Field()
    num_bedrooms = scrapy.Field()
    price = scrapy.Field()
    properties = scrapy.Field()
    amenities = scrapy.Field()
