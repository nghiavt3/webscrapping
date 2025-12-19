import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_tlh'
    mcpcty = 'TLH'
    # Thay thế bằng domain thực tế
    allowed_domains = ['tienlensteel.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://tienlensteel.com.vn/vi/relation/0'] 

    def start_requests(self):
        yield scrapy.Request(
        url=self.start_urls[0],
        callback=self.parse,
        # Thêm meta để kích hoạt Playwright
        meta={'playwright': True}
    )
        
    def parse(self, response):
        # Duyệt qua từng mục tin tức/quan hệ cổ đông
        for item in response.css('.relation-item'):
            
            # 1. Trích xuất và xử lý thời gian
            raw_time = item.css('.relation-item__time::text').get()
            iso_date = None
            publish_time = None
            
            if raw_time:
                try:
                    # Tách chuỗi "01/07/2025 - 16:26:48"
                    # Lấy phần ngày: "01/07/2025"
                    date_part = raw_time.split('-')[0].strip()
                    # Chuyển sang ISO YYYY-MM-DD
                    iso_date = datetime.strptime(date_part, "%d/%m/%Y").strftime("%Y-%m-%d")
                    
                    # Nếu bạn muốn lấy cả giờ:
                    publish_time = raw_time.split('-')[-1].strip()
                except Exception:
                    iso_date = raw_time

            # 2. Trích xuất tiêu đề và Link
            title = item.css('.relation-item__main__title::text').get()
            file_url = item.css('.relation-item__main__title::attr(href)').get()
            
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = str(title)
            e_item['details_raw'] = str(title) +'\n' + str(file_url)
            e_item['date'] = (iso_date)
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
    input_format = '%d/%m/%Y'
    
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
