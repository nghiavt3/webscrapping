import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_oil'
    mcpcty = 'OIL'
    # Thay thế bằng domain thực tế
    allowed_domains = ['pvoil.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.pvoil.com.vn/quan-he-co-dong'] 

    def parse(self, response):
        rows = response.css('.ajaxResponse table tbody tr')
        
        for row in rows:
            # 2. Trích xuất Tiêu đề/Nội dung và Đường dẫn PDF
            # Nội dung nằm trong thẻ <a> ở td thứ hai (có colspan="2")
            # Selector: td:nth-child(2) a
            
            # Lấy Tiêu đề (Nội dung văn bản)
            title = row.css('td:nth-child(2) a::text').get()
            
            # Lấy Đường dẫn PDF (thuộc tính href)
            pdf_link = row.css('td:nth-child(2) a::attr(href)').get()
            
            # 3. Trích xuất Ngày và Giờ công bố
            # Ngày/Giờ nằm trong thẻ <p class="news-date"> ở td thứ ba (thực tế là td thứ tư nếu tính cả td rỗng đầu tiên)
            # Dùng td:last-child để chắc chắn lấy cột cuối cùng
            date_time = row.css('td:last-child p.news-date::text').get()
            
            # Làm sạch dữ liệu
            if title:
                title = title.strip()
            if date_time:
                date_time = date_time.strip()
                date_part = date_time.split('|')[0]
                final_date = date_part.strip()

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(pdf_link)
            e_item['date'] = convert_date_to_iso8601(final_date)             
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
