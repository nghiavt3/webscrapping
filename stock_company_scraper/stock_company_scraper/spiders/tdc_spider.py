import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_tdc'
    mcpcty = 'TDC'
    # Thay thế bằng domain thực tế
    allowed_domains = ['becamextdc.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://becamextdc.com.vn/shareholders/co-dong'] 

    def parse(self, response):
        document_rows = response.css('.main-document table tr')

        for row in document_rows:
            # Trích xuất dữ liệu từ các cột (td)
            date = row.css('td:nth-child(1)::text').get().strip() if row.css('td:nth-child(1)::text').get() else None
            
            # Nội dung có thể chứa khoảng trắng thừa, nên dùng .get().strip()
            content = row.css('td:nth-child(2)::text').get().strip() if row.css('td:nth-child(2)::text').get() else None
            
            # Đường dẫn download là thuộc tính 'href' của thẻ 'a' trong cột thứ 3
            download_link = row.css('td:nth-child(3) a::attr(href)').get()
            
            if date and content: # Đảm bảo đây là hàng dữ liệu hợp lệ
                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = content
                e_item['details_raw'] = str(content) +'\n' + str(download_link)
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
    input_format = '%d-%m-%Y'
    
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
