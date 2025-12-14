import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_tch'
    mcpcty = 'TCH'
    # Thay thế bằng domain thực tế
    allowed_domains = ['hoanghuy.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.hoanghuy.vn/quan-he-co-dong/'] 

    def clean_and_format_date(self, date_pub_raw):
        """Hàm giúp làm sạch chuỗi ngày và chuyển đổi định dạng."""
        if not date_pub_raw:
            return None
            
        # Loại bỏ phần chữ "Cập nhật: " và khoảng trắng thừa
        date_str = date_pub_raw.replace('Cập nhật:', '').strip()
        
        # Thử chuyển đổi định dạng D/M/YYYY sang YYYY-MM-DD
        # Lưu ý: Cần xử lý cả 7/11/2025 (D/M/YYYY) và 28/11/2025 (DD/MM/YYYY)
        try:
            # Thử với định dạng có 1 hoặc 2 chữ số cho ngày và tháng
            input_format = '%d/%m/%Y'
            datetime_object = datetime.strptime(date_str, input_format)
            return datetime_object.strftime('%Y-%m-%d')
        except ValueError:
            return date_str # Trả về chuỗi gốc nếu không thể chuyển đổi

    def parse(self, response):
        for item in response.css('ul.codong li'):
            title = item.css('h3 a::text').get()
            relative_url = item.css('h3 a::attr(href)').get()
            date_raw = item.css('p::text').get()
            # Xử lý Ngày cập nhật
            cleaned_date = self.clean_and_format_date(date_raw)
            

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) + '\n' + str(relative_url)
            e_item['date'] = cleaned_date
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
    input_format = "%d/%m/%Y"    
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
