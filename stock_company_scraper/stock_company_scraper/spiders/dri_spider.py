import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_dri'
    mcpcty= 'DRI'
    # Thay thế bằng domain thực tế
    allowed_domains = ['dri.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://dri.com.vn/categories/thong-bao-64.html'] 

    def parse(self, response):
        # 1. Lấy danh sách tất cả các thông báo (records)
        article_items = response.css('.row.categoryList .col-md-12 .article-item')

        for item in article_items:
            # Trích xuất dữ liệu
            title = item.css('h4 a::text').get()
            url = item.css('h4 a::attr(href)').get()
            date_raw = item.css('h4 span::text').get()
            img_url = item.css('a.img img::attr(src)').get()
            
            # Làm sạch dữ liệu 
            # Loại bỏ ngoặc đơn từ trường ngày
            cleaned_date = date_raw.strip('()') if date_raw else None

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(url)
            e_item['date'] = convert_date_to_iso8601(cleaned_date)               
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
