import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_bqp'
    mcpcty= 'BQP'

    # Thay thế bằng domain thực tế
    allowed_domains = ['bqp.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://bqp.com.vn/quan-he-co-dong/'] 

    # def start_requests(self):
    #     yield scrapy.Request(
    #     url=self.start_urls[0],
    #     callback=self.parse,
    #     # Thêm meta để kích hoạt Playwright
    #     meta={'playwright': True}
    # )
    
    def parse(self, response):
        # Chọn tất cả các khối lớn chứa danh mục (productst-e)
        category_blocks = response.css('div.productst-e')

        for block in category_blocks:
            # 1. Trích xuất Tiêu đề danh mục lớn (Category Title)
            # Đi từ block.css('h2') và lấy text, sau đó loại bỏ các tag con và khoảng trắng
            category_title = block.css('h2 span b::text').get()
            if category_title:
                category_title = category_title.strip()
            
            # Chọn tất cả các mục tài liệu (sukien-item) bên trong khối danh mục
            items = block.css('div.sukien-item')
            
            for item in items:
                # 2. Trích xuất Ngày công bố (Date)
                date_new = item.css('span.date-new::text').get()
                
                # 3. Trích xuất Năm công bố (Year)
                month_new = item.css('span.month-new::text').get()
                
                # Kết hợp ngày/tháng và năm
                full_date = f"{date_new.strip()}/{month_new.strip()}" if date_new and month_new else (date_new or month_new or '').strip()

                # 4. Trích xuất Tiêu đề bài viết/tài liệu (Title)
                title = item.css('div.sukien-title a::text').get()
                if title:
                    title = title.strip()
                
                # 5. Trích xuất Liên kết chi tiết bài viết/tài liệu (Link)
                link = item.css('div.sukien-title a::attr(href)').get()
                
                # 6. Trích xuất Liên kết tải tài liệu (Download Link)
                # Dùng selector: .sharaholder-us-down a[title="Tải tài liệu"]
                download_link = item.css('div.sharaholder-us-down a[title="Tải tài liệu"]::attr(href)').get()

                e_item = EventItem()
                # 2. Trích xuất dữ liệu chi tiết
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['date'] = convert_date_to_iso8601(full_date)
                e_item['summary'] = title
                
                e_item['details_raw'] = str(title) + '\n' + str(link) + '\n' + str(download_link) 
                            
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
