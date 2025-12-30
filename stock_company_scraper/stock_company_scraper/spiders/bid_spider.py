import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_bid'
    mcpcty = 'BID'
    # Thay thế bằng domain thực tế
    allowed_domains = ['bidv.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://bidv.com.vn/vn/quan-he-nha-dau-tu/thong-tin-co-dong/'] 

    def start_requests(self):
        yield scrapy.Request(
        url=self.start_urls[0],
        callback=self.parse,
        # Thêm meta để kích hoạt Playwright
        meta={"playwright": True,
                
                }
    )
        
    def parse(self, response):
        # Lấy tất cả các thẻ <a> chứa item
        items = response.css('div.row.g-2rem > div > a')

        for a_tag in items:
            # Trích xuất dữ liệu bên trong mỗi khối
            raw_date = a_tag.css('p::text').get()
            title = a_tag.css('h5::text').get()
            link = a_tag.css('::attr(href)').get()

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) + '\n'+ str(response.urljoin(link) if link else None)
            e_item['date'] = format_date(raw_date)         
            yield e_item

from datetime import datetime

def format_date( date_str):
        if not date_str:
            return None
        try:
            # BIDV format: DD/MM/YYYY -> ISO: YYYY-MM-DD
            return datetime.strptime(date_str.strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
        except ValueError:
            return date_str