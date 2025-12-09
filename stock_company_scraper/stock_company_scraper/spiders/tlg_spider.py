import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_tlg'
    # Thay thế bằng domain thực tế
    allowed_domains = ['thienlonggroup.com'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://thienlonggroup.com/quan-he-co-dong/tat-ca'] 

    def parse(self, response):
        for item in response.css('.list-news-2 .item'):
            e_item = EventItem()
            e_item['mcp'] = 'TLG'
            e_item['web_source'] = 'thienlonggroup.com'
            e_item['summary'] = item.css('div.title a::text').get().strip()
            e_item['details_raw'] =item.css('div.title a::text').get().strip() +' \n'+ item.css('a.down::attr(href)').get()
            e_item['date'] = convert_date_to_iso8601(item.css('span.date::text').get().strip())               
            yield e_item

from datetime import datetime

def convert_date_to_iso8601(vietnam_date_str):
    """
    Chuyển đổi chuỗi ngày tháng từ định dạng 'DD.MM.YYYY' sang 'YYYY-MM-DD' (ISO 8601).
    
    :param vietnam_date_str: Chuỗi ngày tháng đầu vào, ví dụ: '04.12.2025'
    :return: Chuỗi ngày tháng ISO 8601, ví dụ: '2025-12-04'
    :raises ValueError: Nếu chuỗi đầu vào không đúng định dạng.
    """
    if not vietnam_date_str:
        return None  # Xử lý trường hợp chuỗi rỗng hoặc None

    try:
        # 1. Định nghĩa định dạng đầu vào ('%d.%m.%Y') và parse chuỗi thành đối tượng datetime
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d.%m.%Y')
        
        # 2. Định dạng lại đối tượng datetime thành chuỗi ISO 8601 ('YYYY-MM-DD')
        iso_date_str = date_object.strftime('%Y-%m-%d')
        
        return iso_date_str
    
    except ValueError as e:
        print(f"Lỗi chuyển đổi ngày tháng '{vietnam_date_str}': {e}")
        return None # Trả về None hoặc một giá trị mặc định nếu có lỗi    

