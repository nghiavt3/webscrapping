import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_evg'
    # Thay thế bằng domain thực tế
    allowed_domains = ['everland.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://everland.vn/quan-he-co-dong/cong-bo-thong-tin'] 

    def parse(self, response):
        records = response.css('div.article-left div.article-item')
        
        for record in records:
            
            # Trích xuất dữ liệu thô
            title_raw = record.css('h3 a.title::text').get()
            doc_url_relative = record.css('h3 a.title::attr(href)').get()
            date_raw = record.css('h3 span.date::text').get()
            
            # Tóm tắt thường nằm trong thẻ <p> thứ hai (index 1 nếu tính cả thẻ <p> rỗng đầu tiên)
            # Ta lấy tất cả text nodes của các thẻ <p> và chọn text node thứ hai (index 1)
            description_text_nodes = record.css('p::text').getall()
            description_raw = description_text_nodes[1].strip() if len(description_text_nodes) > 1 else None

            # Làm sạch dữ liệu
            cleaned_title = title_raw.strip() if title_raw else None
            
            # Loại bỏ dấu ngoặc đơn và khoảng trắng thừa trong ngày tháng (ví dụ: (17.11.2025) -> 17.11.2025)
            cleaned_date = date_raw.strip('() \n\r\t') if date_raw else None
            
            # Xử lý URL tương đối
            full_doc_url = response.urljoin(doc_url_relative) if doc_url_relative else None
            e_item = EventItem()
            e_item['mcp'] = 'EVG'
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_title
            e_item['details_raw'] = cleaned_title+ '\n'+ description_raw + '\n' + full_doc_url
            e_item['date'] = convert_date_to_iso8601(cleaned_date)               
            yield e_item

from datetime import datetime

def convert_date_to_iso8601(vietnam_date_str):
    """
    Chuyển đổi chuỗi ngày tháng từ định dạng 'DD.MM.YYYY' sang 'YYYY-MM-DD' (ISO 8601).
    
    :param vietnam_date_str: Chuỗi ngày tháng đầu vào, ví dụ: '04.12.2025'
    :return: Chuỗi ngày tháng ISO 8601, ví dụ: '2025-12-04'
    :raises ValueError: Nếu chuỗi đầu vào không đúng định dạng.
    """
    if not vietnam_date_str:
        return None  # Xử lý trường hợp chuỗi rỗng hoặc None

    try:
        # 1. Định nghĩa định dạng đầu vào ('%d.%m.%Y') và parse chuỗi thành đối tượng datetime
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d.%m.%Y')
        
        # 2. Định dạng lại đối tượng datetime thành chuỗi ISO 8601 ('YYYY-MM-DD')
        iso_date_str = date_object.strftime('%Y-%m-%d')
        
        return iso_date_str
    
    except ValueError as e:
        print(f"Lỗi chuyển đổi ngày tháng '{vietnam_date_str}': {e}")
        return None # Trả về None hoặc một giá trị mặc định nếu có lỗi    

