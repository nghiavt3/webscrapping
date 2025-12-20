import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_fts'
    mcpcty = 'FTS'
    # Thay thế bằng domain thực tế
    allowed_domains = ['fpts.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.fpts.com.vn/quan-he-co-dong/cong-bo-thong-tin/'] 

    def start_requests(self):
        yield scrapy.Request(
        url=self.start_urls[0],
        callback=self.parse,
        # Thêm meta để kích hoạt Playwright
        meta={'playwright': True}
    )
        
    def parse(self, response):
        # Lặp qua danh sách các khối tin tức
        for item in response.css('div.news.fix-news'):
            # 2. Trích xuất Tiêu đề, URL và Ngày đăng cho mỗi mục
            title = item.css('h3.title a.text-title::text').get().strip()
            url = item.css('h3.title a.text-title::attr(href)').get()
            
            # Trích xuất Ngày đăng và làm sạch chuỗi
            date_text = item.css('p.date-news span.datetime::text').get()
            

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n'+ str(url)
            e_item['date'] = convert_date_to_iso8601(date_text)               
            yield e_item

from datetime import datetime

def convert_date_to_iso8601(vietnam_date_str):
    """
    Chuyển đổi chuỗi ngày tháng từ định dạng 'DD/MM/YYYY' sang 'YYYY-MM-DD' (ISO 8601).
    
    :param vietnam_date_str: Chuỗi ngày tháng đầu vào, ví dụ: '20/09/2025'
    :return: Chuỗi ngày tháng ISO 8601, ví dụ: '2025-09-20', hoặc None nếu có lỗi.
    """
    if not vietnam_date_str:
        return None

    # Định dạng đầu vào: Ngày/Tháng/Năm ('%d/%m/%Y')
    input_format = '%d/%m/%Y %H:%M'
    
    # Định dạng đầu ra: Năm-Tháng-Ngày ('%Y-%m-%d') - chuẩn ISO 8601 cho ngày
    output_format = '%Y-%m-%d'

    try:
        # 1. Parse chuỗi đầu vào thành đối tượng datetime
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        
        # 2. Định dạng lại đối tượng datetime thành chuỗi ISO 8601
        iso_date_str = date_object.strftime(output_format)
        
        return iso_date_str
    
    except ValueError as e:
        print(f"⚠️ Lỗi chuyển đổi ngày tháng '{vietnam_date_str}' (phải là DD/MM/YYYY): {e}")
        return None
