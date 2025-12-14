import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_acv'
    # Thay thế bằng domain thực tế
    allowed_domains = ['acv.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://acv.vn/tin-tuc/thong-bao-co-dong'] 

    def parse(self, response):
        notices = response.css('ul.blog-items li.item')
        
        for notice in notices:
            # 2. Trích xuất Tiêu đề và URL
            title_url_element = notice.css('h4.title a')
            
            title = title_url_element.css('::text').get().strip()
            # Lấy URL và nối với base_url nếu nó là relative URL
            relative_url = title_url_element.css('::attr(href)').get()
            full_url = self.start_urls[0] + relative_url

            # 3. Trích xuất Thời gian và Ngày
            datetime_str = notice.css('div.datetime span::text').get()
            
            # Xử lý chuỗi để tách Thời gian và Ngày (ví dụ: '17:51 | 04/12/2025')
            time = None
            date = None
            if datetime_str:
                parts = datetime_str.strip().split('|')
                if len(parts) == 2:
                    time = parts[0].strip()
                    date = parts[1].strip()

            e_item = EventItem()
            e_item['mcp'] = 'ACV'
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(full_url)
            e_item['date'] = convert_date_to_iso8601(date)               
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
