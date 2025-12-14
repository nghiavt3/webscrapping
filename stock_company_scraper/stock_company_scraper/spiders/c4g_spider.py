import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_c4g'
    # Thay thế bằng domain thực tế
    allowed_domains = ['cienco4.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://cienco4.vn/quanhe_codong_cat/quan-he-co-dong/'] 

    def parse(self, response):
        news_items = response.css('div.list-new-4-col > div.col')
        
        for item in news_items:
            # Lấy thẻ <a> bao ngoài cùng
            anchor = item.css('a')
            
            # 2. Trích xuất URL
            url = anchor.css('::attr(href)').get()
            
            # 3. Trích xuất Tiêu đề
            # Sử dụng .get().strip() để loại bỏ khoảng trắng thừa
            title = anchor.css('h3::text').get().strip() if anchor.css('h3::text').get() else None
            
            # 4. Trích xuất Ngày cập nhật
            # Lấy văn bản từ thẻ <p> và loại bỏ tiền tố "Cập nhật ngày " và khoảng trắng
            date_full = anchor.css('div.thoi-gian-qhcd p::text').get()
            date = date_full.replace('Cập nhật ngày', '').strip() if date_full else None

            e_item = EventItem()
            e_item['mcp'] = 'C4G'
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(url)
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
