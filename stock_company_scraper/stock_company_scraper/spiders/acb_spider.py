import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_acb'
    mcpcty = 'ACB'
    # Thay thế bằng domain thực tế
    allowed_domains = ['acb.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://acb.com.vn/nha-dau-tu'] 

    def start_requests(self):
        yield scrapy.Request(
        url=self.start_urls[0],
        callback=self.parse,
        # Thêm meta để kích hoạt Playwright
        meta={"playwright": True,
                
                }
    )
        
    def parse(self, response):
        items = response.css('.item-brochure')

        for item in items:
            raw_text = item.css('.line-2::text').get()
            
            if raw_text:
                raw_text = raw_text.strip()
                # 1. Trích xuất và định dạng ngày (YYYY-MM-DD)
                publish_date = convert_date_to_iso8601(raw_text)
                
                # 2. Làm sạch tiêu đề (loại bỏ phần ngày và ngoặc đơn ở cuối)
                title = re.sub(r'\s*\(\d{2}/\d{2}/\d{4}\)$', '', raw_text).strip()

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title)
            e_item['date'] = publish_date         
            yield e_item

from datetime import datetime

def convert_date_to_iso8601(text):
    # Regex tìm định dạng dd/mm/yyyy bên trong dấu ngoặc đơn
        match = re.search(r'\((\d{2}/\d{2}/\d{4})\)', text)
        if match:
            date_str = match.group(1)
            try:
                # Chuyển từ 18/12/2025 -> đối tượng datetime -> 2025-12-18
                return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
            except ValueError:
                return None
        return None