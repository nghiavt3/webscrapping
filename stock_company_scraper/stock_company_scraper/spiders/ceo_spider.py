import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_ceo'
    # Thay thế bằng domain thực tế
    allowed_domains = ['ceogroup.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://ceogroup.com.vn/cong-bo-thong-tin-sc81'] 

    def parse(self, response):
        # 1. Lấy danh sách tất cả các thông báo (records)
        records = response.css('div.report-item table tbody tr')
        
        for record in records:
            
            # Trích xuất dữ liệu
            # Cột 1: Tiêu đề và URL
            title_raw = record.css('td:nth-child(1) h3.title a::text').get()
            doc_url = record.css('td:nth-child(1) h3.title a::attr(href)').get()
            
            # Cột 2: Ngày đăng
            date_posted = record.css('td:nth-child(2)::text').get()
            
            # Cột 3: URL Tải về
            download_url = record.css('td:nth-child(3) a::attr(href)').get()
            
            # Làm sạch dữ liệu
            cleaned_title = title_raw.strip() if title_raw else None
            cleaned_date = date_posted.strip() if date_posted else None
            e_item = EventItem()
            e_item['mcp'] = 'CEO'
            e_item['web_source'] = 'ceogroup.com.vn'
            e_item['summary'] = cleaned_title
            e_item['details_raw'] = str(cleaned_title) +'\n' + str(doc_url)+ '\n' + str(download_url)
            e_item['date'] = convert_date_to_iso8601(cleaned_date)               
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
