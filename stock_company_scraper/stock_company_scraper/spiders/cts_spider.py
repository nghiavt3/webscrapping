import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_cts'
    mcpcty = 'CTS'
    # Thay thế bằng domain thực tế
    allowed_domains = ['vbse.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.vbse.vn/danh-muc-co-dong/cong-bo-thong-tin/'] 

    def parse(self, response):
        # Lấy tất cả các mục cổ đông
        items = response.css('div.shareholder-item')
        
        for item in items:
            # 2. Trích xuất Tiêu đề, URL và Ngày đăng cho mỗi mục
            title = item.css('h3.shareholder-title a::text').get().strip()
            url = item.css('h3.shareholder-title a::attr(href)').get()
            excerpt= item.css('p.new-excerpt::text').get().strip(),
            # Trích xuất Ngày đăng và làm sạch chuỗi
            date_text = item.css('div.shareholder-item-right p.news-date::text').get().strip()
            

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(excerpt)+'\n' + str(url)
            e_item['date'] = convert_date_to_iso8601(date_text)               
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
