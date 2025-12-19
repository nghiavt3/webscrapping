import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_dgc'
    mcpcty = 'DGC'
    # Thay thế bằng domain thực tế
    allowed_domains = ['ducgiangchem.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://ducgiangchem.vn/category/quan-he-co-dong/thong-bao/'] 

    def parse(self, response):
        # 1. Lặp qua tất cả các bài viết (Mỗi thẻ <article> là một bài viết)
        for article in response.css('article.post'):
            
            # 2. Trích xuất Ngày và Tháng/Năm, sau đó ghép lại
            day = article.css('p.meta span.day::text').get()
            month_year = article.css('p.meta span.month::text').get()
            
            # Làm sạch dữ liệu (loại bỏ khoảng trắng, xuống dòng)
            day_clean = day.strip() if day else ''
            month_year_clean = month_year.strip() if month_year else ''
            
            full_date = f"{day_clean} {month_year_clean}".strip()
            
            # 3. Trích xuất các trường dữ liệu khác
            title = article.css('h2.title-post a::text').get().strip() if article.css('h2.title-post a::text').get() else ''
            url = article.css('h2.title-post a::attr(href)').get()
            summary = article.css('div.entry-post p::text').get().strip() if article.css('div.entry-post p::text').get() else ''
            thumbnail_url = article.css('div.entry-thumb img::attr(src)').get()
            read_more_url = article.css('a.btn_2::attr(href)').get()

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title)+'\n' + str(summary) +'\n' + str(read_more_url)+'\n' + str(url)
            e_item['date'] = convert_date_to_iso(day_clean,month_year_clean)               
            yield e_item

from datetime import datetime

def convert_date_to_iso(day_raw, month_year_raw):
    """
    Chuyển đổi ngày và chuỗi tháng/năm riêng biệt thành định dạng YYYY-MM-DD.
    
    Args:
        day_raw (str): Chuỗi ngày (ví dụ: '11').
        month_year_raw (str): Chuỗi tháng và năm (ví dụ: '2025, Nov ').
        
    Returns:
        str: Ngày tháng đã được định dạng (ví dụ: '2025-11-11').
    """
    # 1. Làm sạch và ghép chuỗi
    day = day_raw.strip()
    month_year = month_year_raw.strip().replace(', ', ' ') # Loại bỏ dấu phẩy và khoảng trắng
    
    # Chuỗi đầy đủ sẽ là: '11 2025 Nov'
    full_date_string = f"{day} {month_year}"
    
    # 2. Định nghĩa định dạng đầu vào (strptime):
    # %d: Ngày trong tháng (01-31)
    # %Y: Năm (2025)
    # %b: Tên tháng viết tắt (Nov)
    input_format = "%d %Y %b"
    
    try:
        # Phân tích cú pháp chuỗi ngày tháng
        date_object = datetime.strptime(full_date_string, input_format)
        
        # 3. Định dạng lại chuỗi theo ISO 8601 (strftime): YYYY-MM-DD
        iso_date = date_object.strftime("%Y-%m-%d")
        
        return iso_date
    except ValueError as e:
        # Xử lý trường hợp không thể phân tích cú pháp (dữ liệu lỗi)
        print(f"Lỗi phân tích cú pháp ngày: {e}")
        return None