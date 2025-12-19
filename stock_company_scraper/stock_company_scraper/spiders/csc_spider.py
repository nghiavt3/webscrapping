import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_csc'
    mcpcty = 'CSC'
    # Thay thế bằng domain thực tế
    allowed_domains = ['cotanagroup.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.cotanagroup.vn/thong-bao-cua-hdqt/'] 

    def parse(self, response):
        # Selector gốc cho bảng. Sử dụng response.css() để lấy danh sách các hàng.
        rows = response.css('table.table > tbody > tr')

        for row in rows:
            # Trích xuất dữ liệu cho từng hàng (từng tài liệu)
            stt = row.css('th::text').get().strip() if row.css('th::text').get() else None
            
            # Tên tài liệu nằm trong div.doc-name
            ten_tai_lieu = row.css('td > div.doc-name::text').get().strip() if row.css('td > div.doc-name::text').get() else None
            
            # Đường dẫn và tên file tải về nằm trong thẻ <a> có class bnt-dl
            link_tai_ve = row.css('td > a.bnt-dl::attr(href)').get()
            ten_file_tai_ve = row.css('td > a.bnt-dl::attr(download)').get()

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = ten_tai_lieu
            e_item['details_raw'] = str(ten_tai_lieu) +'\n' + str(link_tai_ve) +'\n' + str(ten_file_tai_ve)
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
