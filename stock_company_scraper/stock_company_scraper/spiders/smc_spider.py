import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_smc'
    mcpcty = 'SMC'
    # Thay thế bằng domain thực tế
    allowed_domains = ['smc.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://smc.vn/quan-he-co-dong/'] 

    def parse(self, response):
        # Chọn tất cả các bài đăng tin tức chính
        posts = response.css('.blog-posts .post')

        for post in posts:
            # 1. Trích xuất Ngày công bố
            date_raw = post.css('.post-meta .meta-date::text').get()
            # Loại bỏ ký tự không cần thiết và khoảng trắng. Kết quả: '8/12/2025'
            date = date_raw.replace('\n', '').strip() if date_raw else '' 
            
            # 2. Trích xuất Tiêu đề
            # Chọn thẻ <a> bên trong <h2>.entry-title và lấy văn bản (::text)
            # Sử dụng .getall() và nối chúng lại nếu có thẻ <img> (như trong bài đầu tiên)
            title_parts = post.css('h2.entry-title a::text').getall()
            # Nối các phần (để xử lý trường hợp có thẻ <img> xen kẽ)
            title = " ".join([p.strip() for p in title_parts if p.strip()])
            
            # 3. Trích xuất URL chi tiết (Link)
            # Có hai vị trí cho link: ở h2.entry-title và ở nút "Chi tiết..."
            # Ta chọn link ở h2.entry-title vì nó chứa link bài viết chính
            link = post.css('h2.entry-title a::attr(href)').get()
            
            # 4. Trích xuất Tóm tắt / Đoạn trích (Post Excerpt)
            # Lấy nội dung văn bản bên trong <p.post-excerpt>
            excerpt = post.css('p.post-excerpt::text').get()
            excerpt = excerpt.strip() if excerpt else ''

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(excerpt)+'\n' +str(link)
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
