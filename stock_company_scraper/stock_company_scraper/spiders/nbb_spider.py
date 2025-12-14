import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_nbb'
    mcpcty = 'NBB'
    # Thay thế bằng domain thực tế
    allowed_domains = ['nbb.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['http://nbb.com.vn/vi-vn/zone/557/item/1871/item.cco'] 

    def parse(self, response):
        # 1. Chọn khối chứa "Các tin khác"
        other_items_block = response.css('div.otheritem')
        
        # 2. Chọn tất cả các mục tin tức riêng lẻ (nằm trong thẻ <h5>)
        news_items = other_items_block.css('h5')

        for item in news_items:
            # 3. Trích xuất Tiêu đề (nội dung text của thẻ <a>)
            # Dùng ::text để lấy nội dung bên trong <a>, sau đó dùng .get().strip() để làm sạch khoảng trắng thừa.
            title = item.css('a::text').get().strip()

            # 4. Trích xuất Đường dẫn tương đối (thuộc tính href của thẻ <a>)
            # Lấy thuộc tính 'href' của thẻ <a>
            relative_url = item.css('a::attr(href)').get()
            
            # (Tùy chọn) Xây dựng URL tuyệt đối nếu cần
            # absolute_url = response.urljoin(relative_url)

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(relative_url)
            e_item['date'] = None              
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
        print(f"⚠️ Lỗi chuyển đổi ngày tháng '{vietnam_date_str}' (phải là DD/MM/YYYY): {e}")
        return None
