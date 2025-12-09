import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
from scrapy_playwright.page import PageMethod
import scrapy
class EventSpider(scrapy.Spider):
    name = 'event_vic'
    # Thay thế bằng domain thực tế
    allowed_domains = ['vingroup.net'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://vingroup.net/quan-he-co-dong/cong-bo-thong-tin'] 

    def start_requests(self):
        yield scrapy.Request(
            url='https://vingroup.net/quan-he-co-dong/cong-bo-thong-tin',
            callback=self.parse,
            meta={
                'playwright': True,
                # CHUYỂN 'headless': False VÀO ĐÂY
                'playwright_launch_kwargs': {
                    'headless': False,  # Chế độ Headful (hiển thị cửa sổ) để tăng khả năng qua mặt bot
                },
                # 'playwright_context_kwargs': { ... }, # Không cần thiết lập nếu không có tham số khác
                'playwright_page_methods': [
                    PageMethod('wait_for_selector', 'div.news-list-container') 
                ]
            }
        )


    def parse(self, response):
        # 1. Chọn tất cả các mục tài liệu
        records = response.css('div.ir-document-item')
        
        for record in records:
            # Tiêu đề: Lấy nội dung text trong thẻ span nằm trong thẻ a
            title_raw = record.css('a span::text').get()
            
            # URL Tài liệu: Lấy giá trị href của thẻ a
            document_url_relative = record.css('a::attr(href)').get()
            
            # Ngày công bố: Lấy nội dung text của thẻ em
            date_raw = record.css('em::text').get()
            
            # Làm sạch dữ liệu
            # Sử dụng .strip() để loại bỏ khoảng trắng, ký tự ngắt dòng thừa.
            cleaned_title = title_raw.strip() if title_raw else None
            cleaned_date = date_raw.strip() if date_raw else None

            e_item = EventItem()
            # 2. Trích xuất dữ liệu chi tiết
            e_item['mcp'] = 'VIC'
            e_item['web_source'] = self.allowed_domains[0]
            e_item['date'] = convert_date_to_iso8601(cleaned_date)
            e_item['summary'] = cleaned_title
            
            e_item['details_raw'] = cleaned_title + '\n' + document_url_relative
                         
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
