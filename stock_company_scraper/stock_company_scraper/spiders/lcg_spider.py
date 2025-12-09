import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_lcg'
    # Thay thế bằng domain thực tế
    allowed_domains = ['lizen.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://lizen.vn/vi/document-category/thong-bao-co-dong?page=1'] 

    def parse(self, response):
        # 1. Chọn tất cả các hàng dữ liệu (<tr>) trong tbody
        records = response.css('table.data-recruitment tbody tr')
        
        for record in records:
            # Tiêu đề: Lấy nội dung text của thẻ <a> trong cột đầu tiên
            title = record.css('td.col-item1 a::text').get().strip()
            
            # URL Bài viết: Lấy href của thẻ <a> trong cột đầu tiên
            article_url_relative = record.css('td.col-item1 a::attr(href)').get()
            
            # Ngày Công bố: Cột thứ 2
            date = record.css('td:nth-child(2)::text').get().strip()
            
            # Dạng Tập tin: Cột thứ 3
            file_type = record.css('td:nth-child(3)::text').get().strip()
            
            # URL Tải về: Lấy href của thẻ <a> trong cột thứ 5 (TẢI VỀ)
            download_url_relative = record.css('td:nth-child(5) a::attr(href)').get()

            e_item = EventItem()
            # 2. Trích xuất dữ liệu chi tiết
            e_item['mcp'] = 'LCG'
            e_item['web_source'] = self.allowed_domains[0]
            e_item['date'] = convert_date_to_iso8601(date)
            e_item['summary'] = title
            
            e_item['details_raw'] = title + '\n' + article_url_relative + '\n' + download_url_relative
                         
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
