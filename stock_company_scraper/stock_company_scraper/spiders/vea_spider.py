import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_vea'
    mcpcty = 'VEA'
    # Thay thế bằng domain thực tế
    allowed_domains = ['veamcorp.com'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['http://veamcorp.com/quan-he-co-dong/cong-bo-thong-tin-114.html'] 

    def parse(self, response):
        # 1. Lấy danh sách tất cả các thông báo (records)
        # Chọn tất cả các thẻ DIV là "Featured News" (có class box-catnew)
        featured_news_cards = response.css('div.col-md-9 .card.box-catnew')

        for article in featured_news_cards:
            # 1. Trích xuất Tiêu đề và URL từ thẻ <a> có class 'title-new'
            title_tag = article.css('.card-body .title-new')
            title_raw = title_tag.css('::text').get()
            url = title_tag.css('::attr(href)').get()
            
            # Khắc phục lỗi 'NoneType' bằng cách bỏ qua nếu thiếu Tiêu đề hoặc URL
            if not title_raw or not url:
                # Nếu không có Tiêu đề hoặc URL, không thể tạo item hợp lệ
                continue
                
            title = title_raw.strip()
            
            # 2. Trích xuất Ngày đăng
            date_raw = article.css('.card-body .text-date-new::text').get()
            # Xử lý để loại bỏ chuỗi "Ngày đăng:"
            date_pub = date_raw.replace('Ngày đăng:', '').strip() if date_raw else None
            
            # 3. Trích xuất URL hình ảnh
            image_url = article.css('.img-thumb-new img::attr(src)').get()

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(url)
            e_item['date'] = convert_date_to_iso8601(date_pub)               
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
