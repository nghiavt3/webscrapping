import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_vgi'
    mcpcty= 'VGI'
    # Thay thế bằng domain thực tế
    allowed_domains = ['viettelglobal.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://viettelglobal.com.vn/quan-he-co-dong'] 
    # Ghi đè cấu hình CHỈ CHO SPIDER NÀY
    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
    }
    def start_requests(self):
        yield scrapy.Request(
        url=self.start_urls[0],
        callback=self.parse,
        # Thêm meta để kích hoạt Playwright
        meta={'playwright': True}
    )

    def parse(self, response):
        # Lặp qua từng mục tài liệu
        document_items = response.css('.document-list-item')

        for item in document_items:
            # Trích xuất dữ liệu
            date_raw = item.css('datetime::text').get()
            title = item.css('h3 a::text').get()
            url = item.css('h3 a::attr(href)').get()
            
            # Làm sạch dữ liệu (loại bỏ khoảng trắng/xuống dòng)
            cleaned_title = title.strip() if title else None
            cleaned_date = date_raw.strip() if date_raw else None

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_title
            e_item['details_raw'] = str(cleaned_title) +'\n' + str(url)
            e_item['date'] = convert_date_to_iso8601(cleaned_date)               
            yield e_item

from datetime import datetime

def convert_date_to_iso8601(vietnam_date_str):
    """
    Chuyển đổi chuỗi ngày tháng từ định dạng 'DD.MM.YYYY' sang 'YYYY-MM-DD' (ISO 8601).
    
    :param vietnam_date_str: Chuỗi ngày tháng đầu vào, ví dụ: '20.09.2025'
    :return: Chuỗi ngày tháng ISO 8601, ví dụ: '2025-09-20', hoặc None nếu có lỗi.
    """
    if not vietnam_date_str:
        return None

    # Định dạng đầu vào: Ngày/Tháng/Năm ('%d/%m/%Y')
    input_format = '%d.%m.%Y'
    
    # Định dạng đầu ra: Năm-Tháng-Ngày ('%Y-%m-%d') - chuẩn ISO 8601 cho ngày
    output_format = '%Y-%m-%d'

    try:
        # 1. Parse chuỗi đầu vào thành đối tượng datetime
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        
        # 2. Định dạng lại đối tượng datetime thành chuỗi ISO 8601
        iso_date_str = date_object.strftime(output_format)
        
        return iso_date_str
    
    except ValueError as e:
        print(f"⚠️ Lỗi chuyển đổi ngày tháng '{vietnam_date_str}' (phải là DD.MM.YYYY): {e}")
        return None
