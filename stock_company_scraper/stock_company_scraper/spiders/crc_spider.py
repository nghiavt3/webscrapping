import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_crc'
    mcpcty = 'CRC'
    # Thay thế bằng domain thực tế
    allowed_domains = ['createcapital.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://createcapital.vn/quan-he-co-dong-crc.htm'] 

    def parse(self, response):
        for item in response.css('div.item'):
            # Vì toàn bộ nội dung nằm trong thẻ <a>, ta trích xuất từ nó
            link_selector = item.css('a')

            # Trích xuất URL chi tiết
            url = link_selector.css('::attr(href)').get()
            
            # Trích xuất Tiêu đề từ thẻ h3
            title_raw = link_selector.css('h3.dot3::text').get()
            
            # Trích xuất Tóm tắt từ thẻ p
            summary_raw = link_selector.css('p::text').get()

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title_raw.strip()
            e_item['details_raw'] = str(title_raw.strip()) +'\n' + str(summary_raw.replace('[...]', '').strip()) +'\n' +str(url)
            e_item['date'] = None               
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
