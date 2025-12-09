import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_g36'
    # Thay thế bằng domain thực tế
    allowed_domains = ['36corp.com'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://36corp.com/quan-he-co-dong/cong-bo-thong-tin/'] 

    def parse(self, response):
        # 1. Lấy danh sách tất cả các thông báo (records)
       posts = response.css('article')
        
       for post in posts:
            # 2. Trích xuất Ngày công bố
            # Loại bỏ khoảng trắng
            date = post.css('.entry-meta a.entry-date::text').get()
            if date:
                date = date.strip()
            
            # 3. Trích xuất Tiêu đề
            # Loại bỏ khoảng trắng và các ký tự đặc biệt nếu có
            title = post.css('.entry-title a::text').get()
            if title:
                title = title.strip().replace('\xa0', ' ')
            
            # 4. Trích xuất URL
            url = post.css('.entry-title a::attr(href)').get()

            e_item = EventItem()
            e_item['mcp'] = 'G36'
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
