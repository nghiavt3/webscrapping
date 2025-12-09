# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class EventItem(scrapy.Item):
    # define the fields for your item here like:
    mcp = scrapy.Field()
    details_raw = scrapy.Field()
    summary = scrapy.Field()
    date = scrapy.Field()
    details_clean = scrapy.Field()
    web_source = scrapy.Field()
    pass
