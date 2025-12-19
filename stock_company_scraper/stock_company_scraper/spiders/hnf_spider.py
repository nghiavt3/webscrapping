import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_hnf'
    mcpcty = 'HNF'
    # Thay thế bằng domain thực tế
    allowed_domains = ['huunghi.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://huunghi.com.vn/blogs/quan-he-co-dong'] 

    # def start_requests(self):
    #     yield scrapy.Request(
    #     url=self.start_urls[0],
    #     callback=self.parse,
    #     # Thêm meta để kích hoạt Playwright
    #     meta={'playwright': True}
    # )
        
    def parse(self, response):
        # Chọn tất cả các thẻ li chứa bài viết
        items = response.css('ul#category_main li[data-type="blog"]')
        
        for item in items:
            # 1. Trích xuất tiêu đề
            title = item.css('h3.echbay-blog-title a::text').get()
            
            # 2. Trích xuất link bài viết (để vào lấy link PDF sau này nếu cần)
            post_link = item.css('h3.echbay-blog-title a::attr(href)').get()
            
            # 3. Trích xuất ngày tháng
            # Ngày nằm trong span .echbay-blog-ngay, cần loại bỏ icon và khoảng trắng
            raw_date = item.css('span.echbay-blog-ngay::text').get()
            
            iso_date = None
            if raw_date:
                try:
                    clean_date = raw_date.strip()
                    # Định dạng trên web: 20/10/2025
                    date_obj = datetime.strptime(clean_date, '%d/%m/%Y')
                    iso_date = date_obj.strftime('%Y-%m-%d')
                except ValueError:
                    iso_date = None

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title.strip()
            e_item['details_raw'] = str(title.strip()) +'\n' + str(post_link)
            e_item['date'] = (iso_date)
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
