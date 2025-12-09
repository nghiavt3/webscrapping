import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_pac'
    mcpcty = 'PAC'
    # Thay thế bằng domain thực tế
    allowed_domains = ['pinaco.com'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.pinaco.com/co-dong/thong-tin-khac-68.html'] 

    def parse(self, response):
        # 1. Lấy danh sách tất cả các thông báo (records)
        items = response.css('.colum-list li')

        for item in items:
            # 1. Trích xuất Tiêu đề và URL
            title = item.css('p a::text').get()
            url = item.css('p a::attr(href)').get()

            # 2. Trích xuất Ngày và Số lần tải về từ thẻ <p>
            # Lấy tất cả các đoạn text node trong thẻ <p>
            text_nodes = item.css('p::text').getall()
            
            date = None
            downloads = None
            
            # Text node chứa ngày và số lần tải thường là phần tử thứ 2 trong list (index 2)
            if len(text_nodes) > 2:
                # Ví dụ: ' 2025-11-14 | '
                raw_info = text_nodes[2] 
                
                # Làm sạch và tách lấy Ngày công bố
                # Tách chuỗi bằng ký tự '|'
                parts = [p.strip() for p in raw_info.split('|')]
                
                if parts and parts[0]:
                    date = parts[0].strip() # Lấy phần ngày

            # 3. Trích xuất Số lần tải về (cách riêng biệt, dễ hơn)
            # Số lần tải về nằm trong thẻ <b>
            downloads_count = item.css('p b::text').get()
            
            # Trích xuất đơn vị "Lần tải về"
            downloads_unit_raw = item.css('p::text').getall()
            downloads_unit = ""
            if len(downloads_unit_raw) > 3:
                # Đoạn text chứa đơn vị thường là phần tử thứ 4 (index 3)
                downloads_unit = downloads_unit_raw[3].strip() 

            if downloads_count and downloads_unit:
                # Kết hợp số và đơn vị
                downloads = f"{downloads_count.strip()} {downloads_unit}"


            # Làm sạch Tiêu đề
            if title:
                title = title.strip()

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = title +'\n' + url
            e_item['date'] = date               
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
