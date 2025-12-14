import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_hnm'
    mcpcty = 'HNM'
    # Thay thế bằng domain thực tế
    allowed_domains = ['hanoimilk.com'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['http://www.hanoimilk.com/blogs/dai-hoi-co-dong'] 

    def parse(self, response):
        # 1. Lấy danh sách tất cả các thông báo (records)
        article_items = response.css('.article-item')

        for item in article_items:
            # Trích xuất dữ liệu
            title_raw = item.css('.article-title a::text').get()
            url = item.css('.article-title a::attr(href)').get()
            img_url = item.css('.article-img img::attr(src)').get()
            date_nodes = item.css('.article-date::text').getall()

            # Lọc bỏ các khoảng trắng, xuống dòng, và node text rỗng do thẻ SVG gây ra
            # Thẻ SVG thường không có text, nên text node của ngày tháng là node cuối cùng có nội dung.
            date_raw = [text.strip() for text in date_nodes if text.strip()]

            # Lấy ngày tháng chính xác (thường là phần tử cuối cùng sau khi lọc)
            date = date_raw[-1] if date_raw else None
            
            author_raw = item.css('.article-author::text').get()
            
            # Làm sạch dữ liệu (cắt bỏ khoảng trắng dư thừa)
            title = title_raw.strip() if title_raw else None
            author = author_raw.split('-->')[-1].strip() if author_raw else None
            
            # Xử lý URL tuyệt đối (nếu cần)
            absolute_url = response.urljoin(url) if url else None
            absolute_img_url = response.urljoin(img_url) if img_url else None

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(absolute_url)
            e_item['date'] = convert_date_to_sqlite_format(date)
            yield e_item

from datetime import datetime

def convert_date_to_sqlite_format(date_string):
    """
    Chuyển đổi chuỗi ngày tháng từ định dạng 'DD/MM/YY' sang 'YYYY-MM-DD'.
    
    Ví dụ: '08/09/25' sẽ thành '2025-09-08'.
    
    Args:
        date_string (str): Chuỗi ngày tháng ở định dạng DD/MM/YY.
        
    Returns:
        str: Chuỗi ngày tháng ở định dạng YYYY-MM-DD (ISO 8601).
    """
    try:
        # 1. Định nghĩa định dạng đầu vào (Input Format):
        # %d: Ngày (Day), %m: Tháng (Month), %y: Năm 2 chữ số (Year YY)
        input_format = '%d/%m/%y'
        
        # 2. Phân tích chuỗi ngày tháng: 
        # Chuyển chuỗi thành đối tượng datetime.
        date_object = datetime.strptime(date_string, input_format)
        
        # 3. Định dạng đầu ra (Output Format):
        # %Y: Năm 4 chữ số (YYYY), %m: Tháng, %d: Ngày
        output_format = '%Y-%m-%d'
        
        # 4. Chuyển đổi đối tượng datetime sang chuỗi định dạng mong muốn (SQLite Format)
        sqlite_date_string = date_object.strftime(output_format)
        
        return sqlite_date_string
        
    except ValueError as e:
        print(f"Lỗi chuyển đổi ngày tháng: {e}")
        return None
