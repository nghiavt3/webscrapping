import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_tcx'
    mcpcty = 'TCX'
    # Thay thế bằng domain thực tế
    allowed_domains = ['tcbs.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.tcbs.com.vn/nha-dau-tu/quan-he-nha-dau-tu/cong-bo-thong-tin/'] 

    def parse(self, response):
        
        # Lặp qua từng item tin tức
        for post in response.css('div.custom-post-item-news'):
            
            # 1. Trích xuất tiêu đề (nằm trong thẻ a bên trong h2)
            title = post.css('h2 a::text').get()
            
            # 2. Trích xuất đường dẫn bài viết
            url = post.css('h2 a::attr(href)').get()
            
            # 3. Trích xuất ngày đăng
            raw_date = post.css('div.post-date::text').get()
            
            if title:
                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = title.strip()
                e_item['details_raw'] = str(title.strip()) +'\n' + str(url)
                e_item['date'] = convert_date_to_iso8601(raw_date)               
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
