import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_hid'
    mcpcty = 'HID'
    # Thay thế bằng domain thực tế
    allowed_domains = ['halcom.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://halcom.vn/category/quan-he-co-dong/cong-bo-thong-tin/'] 

    def parse(self, response):
        # Container chứa tất cả các bài viết/thông báo
        posts_container = response.css('div.elementor-posts-container')
        
        # 1. Lặp qua tất cả các bài viết (Mỗi thẻ <article> là một thông báo)
        for post in posts_container.css('article.elementor-post'):
            
            # Trích xuất Tiêu đề
            title_element = post.css('h3.elementor-post__title a::text').get()
            title = title_element.strip() if title_element else None
            
            # Trích xuất URL
            url = post.css('h3.elementor-post__title a::attr(href)').get()
            
            # Trích xuất Tóm tắt (Excerpt)
            summary_element = post.css('div.elementor-post__excerpt p::text').get()
            summary = summary_element.strip() if summary_element else None

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(url)
            e_item['date'] = 'None'              
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
    input_format = '%H:%M %d/%m/%Y'
    
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
