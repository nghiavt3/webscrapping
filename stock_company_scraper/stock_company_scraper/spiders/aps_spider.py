import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_aps'
    mcpcty = 'APS'
    # Thay thế bằng domain thực tế
    allowed_domains = ['apec.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://apec.com.vn/quan-he-co-dong/thong-tin-co-dong/'] 

    def parse(self, response):
        # Lặp qua từng hàng trong tbody của bảng
        for row in response.css('div.analysis-content table tbody tr'):
            # Trích xuất văn bản trực tiếp từ các ô td
            date = (row.css('td.date::text').get() or "").strip()
            content = (row.css('td.content::text').get() or "").strip()
            
            # Trích xuất tất cả các link trong ô td.file (vì có thể có nhiều thẻ a)
            links = row.css('td.file a::attr(href)').getall()
            

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = content
            e_item['details_raw'] = str(content) +'\n' + str(links)
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
