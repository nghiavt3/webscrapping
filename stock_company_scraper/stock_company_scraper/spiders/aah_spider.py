import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_aah'
    # Thay thế bằng domain thực tế
    allowed_domains = ['thanhopnhat.com'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://thanhopnhat.com/category/quan-he-co-dong/'] 

    def parse(self, response):
        post_items = response.css('div.col.post-item')
        
        for item in post_items:
            # 2. Trích xuất Tiêu đề và URL
            title_selector = item.css('h5.post-title a')
            title = title_selector.css('::text').get().strip() if title_selector else None
            url = title_selector.css('::attr(href)').get() if title_selector else None
            
            # 3. Trích xuất Ngày đăng (cần strip() để loại bỏ khoảng trắng)
            date = item.css('div.post-meta::text').get().strip() if item.css('div.post-meta::text') else None
            
            # 4. Trích xuất Tóm tắt (cần strip() để loại bỏ khoảng trắng và ký tự thừa)
            excerpt = item.css('p.from_the_blog_excerpt::text').get()
            if excerpt:
                # Loại bỏ ký tự thừa (như dấu chấm lửng/khoảng trắng)
                excerpt = excerpt.strip().replace('\xa0', '').replace('\n', '')

            e_item = EventItem()
            e_item['mcp'] = 'AAH'
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = title +'\n'+ excerpt +'\n' + url
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
