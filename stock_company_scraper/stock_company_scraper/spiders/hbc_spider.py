import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_hbc'
    # Thay thế bằng domain thực tế
    allowed_domains = ['hbcg.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://hbcg.vn/report/news.html'] 

    def parse(self, response):
        news_items = response.css('div.gridBlock-content a')
    
        for item in news_items:
            # Lấy tiêu đề và loại bỏ ký tự trắng thừa
            title = item.css('p.txt7::text').get().strip() if item.css('p.txt7::text').get() else None
            
            # Lấy mô tả/tóm tắt
            summary = item.css('p.gridBlock-description::text').get().strip() if item.css('p.gridBlock-description::text').get() else None
            
            # Lấy ngày và loại bỏ phần 'Cập nhật ngày: '
            date_raw = item.css('p.date-info::text').get()
            date_clean = date_raw.replace('Cập nhật ngày:', '').strip() if date_raw else None
            
            # Lấy link PDF
            pdf_url = item.css('::attr(href)').get()

            e_item = EventItem()
            e_item['mcp'] = 'HBC'
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' +str(summary) +'\n' + str(pdf_url)
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
