import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_sbs'
    # Thay thế bằng domain thực tế
    allowed_domains = ['sbsc.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://sbsc.com.vn/Annoucement.aspx'] 

    def parse(self, response):
        # 1. Lấy danh sách tất cả các thông báo (records)
        news_items = response.css('div.main-content ul.list > li')
        
        for item in news_items:
            # 2. Trích xuất Tiêu đề, URL và Ngày đăng cho mỗi mục
            title = item.css('h3.title a::text').get()
            url = item.css('h3.title a::attr(href)').get()
            
            # Trích xuất Ngày đăng và làm sạch chuỗi
            date_text = item.css('p.date::text').getall()
            # Lấy phần tử cuối cùng và loại bỏ khoảng trắng dư thừa
            # Ví dụ: [' Đăng ngày:', '03/12/2025 '] -> '03/12/2025 '
            date = date_text[-1].strip() if date_text else None

            e_item = EventItem()
            e_item['mcp'] = 'SBS'
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = title +'\n' + url
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
