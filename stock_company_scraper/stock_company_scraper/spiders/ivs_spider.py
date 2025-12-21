import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_ivs'
    mcpcty = 'IVS'
    # Thay thế bằng domain thực tế
    allowed_domains = ['gtjai.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://gtjai.com.vn/cate_disclosure/cong-bo-thong-tin/'] 

    def parse(self, response):
        # Lặp qua từng hàng trong bảng
        for row in response.css('.table_bp tbody tr'):
            # 1. Trích xuất dữ liệu thô
            title = row.css('td.bao_cao a span::text').get()
            link = row.css('td.bao_cao a::attr(href)').get()
            date_raw = row.css('td:nth-child(2)::text').get()

            # 2. Xử lý ngày tháng sang định dạng ISO (YYYY-MM-DD)
            iso_date = None
            if date_raw:
                try:
                    # Làm sạch khoảng trắng thừa
                    clean_date_str = date_raw.strip()
                    # Parse từ định dạng DD/MM/YYYY
                    dt_obj = datetime.strptime(clean_date_str, '%d/%m/%Y')
                    iso_date = dt_obj.strftime('%Y-%m-%d')
                except ValueError:
                    iso_date = date_raw.strip()
            

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n'  + str(link)
            e_item['date'] = (iso_date)               
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
