import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_hdc'
    mcpcty = 'HDC'
    # Thay thế bằng domain thực tế
    allowed_domains = ['hodeco.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://hodeco.vn/shareholder/1'] 

    def parse(self, response):
        # 1. Selector chính: Chọn tất cả các khối tài liệu riêng lẻ
        # Các tài liệu đều nằm trong div.item-cd, và tất cả nằm trong div.wrap-cd
        documents = response.css('div.wrap-cd div.item-cd')

        for doc in documents:
            # 2. Trích xuất Năm (Năm chỉ xuất hiện ở thẻ <h3> trong item đầu tiên của nhóm)
            # Nếu thẻ <h3> tồn tại, nó sẽ được trích xuất
            year_raw = doc.css('h3::text').get()
            # Xử lý làm sạch: loại bỏ chữ 'Năm' và khoảng trắng
            year = year_raw.replace('Năm', '').strip() if year_raw else None

            # 3. Trích xuất Tiêu đề và URL
            # Tiêu đề và URL nằm trong thẻ <a> bên trong span.name-cd
            title_selector = doc.css('span.name-cd a')
            title = title_selector.css('::text').get().strip() if title_selector else None
            url = title_selector.css('::attr(href)').get()
            
            # 4. Trích xuất Ngày/Giờ
            # Ngày giờ nằm trong span.date-cd
            date_time_raw = doc.css('span.date-cd::text').get()
            # Xử lý làm sạch: loại bỏ ký tự ngoặc đơn và khoảng trắng dư thừa
            date_time = date_time_raw.strip('() \n') if date_time_raw else None

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) + '\n' + str(url)
            e_item['date'] = date_time
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
    input_format = "%d/%m/%Y"    
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
