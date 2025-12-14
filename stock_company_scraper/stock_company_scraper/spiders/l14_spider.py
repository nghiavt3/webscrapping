import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_l14'
    mcpcty = 'L14'
    # Thay thế bằng domain thực tế
    allowed_domains = ['licogi14.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://licogi14.vn/danh-muc/quan-he-co-dong/'] 

    def parse(self, response):
        posts = response.css('div.col.post-item') 

        for post in posts:
            # 1. Trích xuất URL Bài viết (Link)
            # Lấy thuộc tính 'href' của thẻ <a> chứa toàn bộ bài viết
            link = post.css('a.plain::attr(href)').get()

            # 2. Trích xuất Tiêu đề (Title)
            # Chọn thẻ <h5> có class 'post-title is-large'
            title = post.css('h5.post-title.is-large::text').get().strip() if post.css('h5.post-title.is-large::text').get() else None

            # 3. Trích xuất Tóm tắt/Nội dung trích dẫn (Excerpt)
            # Chọn thẻ <p> có class 'from_the_blog_excerpt'
            excerpt = post.css('p.from_the_blog_excerpt::text').get().strip() if post.css('p.from_the_blog_excerpt::text').get() else None
            
            # 4. Trích xuất URL Hình ảnh đại diện (Image URL)
            # Chọn thẻ <img> và lấy thuộc tính 'src'
            image_url = post.css('div.box-image img::attr(src)').get()

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(excerpt)  +'\n' + str(link)
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
