import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_msr'
    # Thay thế bằng domain thực tế
    allowed_domains = ['masanhightechmaterials.com'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://masanhightechmaterials.com/vi/investor_category/thong-bao-cong-ty/'] 

    def parse(self, response):
        release_items = response.css('div.releases-item')
        
        for item in release_items:
            # Lấy ngày công bố, loại bỏ khoảng trắng và icon
            date_raw = item.css('div.date::text').getall()
            # Lấy phần tử cuối cùng (thường là ngày sau icon) và loại bỏ khoảng trắng
            date = date_raw[-1].strip() if date_raw else None
            
            # Lấy phần tử chính
            main_title_selector = item.css('h4 a')
            main_title = main_title_selector.css('::text').get()
            main_url = main_title_selector.css('::attr(href)').get()
            
            # Loại bỏ khoảng trắng và ký tự thừa (như \xa0)
            if main_title:
                main_title = main_title.strip().replace('\xa0', ' ')
            
            # Kiểm tra xem có danh sách PDF con không
            pdf_list = item.css('ol.pdf-list-custom li a')


            e_item = EventItem()
            e_item['mcp'] = 'MSR'
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = main_title
            e_item['details_raw'] = str(main_title) +'\n' + str(main_url)
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
