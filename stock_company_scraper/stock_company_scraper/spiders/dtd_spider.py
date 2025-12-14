import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_dtd'
    mcpcty = 'DTD'
    # Thay thế bằng domain thực tế
    allowed_domains = ['thanhdathanam.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://thanhdathanam.vn/quan-he-co-dong/thong-tin-cong-bo/'] 

    def parse(self, response):
        posts = response.css('div.new-list div.news-post')

        for post in posts:
            # 2. Tiêu đề và URL bài viết
            # Chọn thẻ <a> bên trong <h2>.title và lấy text (tiêu đề) và thuộc tính href (URL)
            title_selector = post.css('h2.title a')
            title = title_selector.css('::text').get().strip() if title_selector else None
            url = title_selector.css('::attr(href)').get()
            
            # 3. Ngày đăng
            # Chọn thẻ <span> cuối cùng (chứa biểu tượng lịch) trong div.date-time, sau đó lấy văn bản.
            date_raw = post.css('div.date-time span:last-child::text').get()
            # Loại bỏ biểu tượng unicode hoặc khoảng trắng dư thừa
            date_clean = date_raw.strip() if date_raw else None
            
            # 4. URL hình ảnh (Nếu có)
            # Chọn thẻ <img> trong div.img-post và lấy thuộc tính 'src'
            image_url_relative = post.css('div.img-post img::attr(src)').get()
            
            # Xây dựng URL hình ảnh đầy đủ (absolute URL)
            image_url = response.urljoin(image_url_relative) if image_url_relative else None
            
            # 5. Tóm tắt (Đoạn text đầu tiên trong thẻ <p>)
            # Chọn thẻ <p> và lấy văn bản của node con đầu tiên (text node)
            # Sử dụng XPath để dễ dàng lấy node text đầu tiên:
            # post.xpath('./p/text()').get().strip() if post.xpath('./p/text()').get() else None
            # Hoặc CSS Selector cho đoạn text đầu tiên:
            summary_raw = post.css('p::text').get()
            summary = summary_raw.strip() if summary_raw else None
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) + '\n' + str(summary) + '\n' + str(url)
            e_item['date'] = convert_date_to_iso8601(date_clean)
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
    input_format = "%d/%m/%Y"    
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
