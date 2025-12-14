import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
from scrapy.selector import Selector
import re
class EventSpider(scrapy.Spider):
    name = 'event_dgt'
    # Thay thế bằng domain thực tế
    allowed_domains = ['dgtc.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://dgtc.vn/co-dong/thong-tin-co-dong'] 

    def parse(self, response):
       # response = Selector(text=html_content)
    
        # 1. Trích xuất danh sách các năm có dữ liệu
        #available_years = response.css('select.select-year option[value!="-- Chọn năm --"]::attr(value)').getall()
        
        # 2. Trích xuất thông tin từng tin tức
        #news_data = []
        
        # Lặp qua từng khối tin tức
        for item in response.css('div.row.gird-padding-custom'):
            
            # Lấy ngày (VD: '02')
            day = item.css('.gird-padding-custom-day::text').get()
            
            # Lấy tháng-năm (VD: '12-2025'). Cần lấy text trực tiếp trong div cha, bỏ qua span con
            month_year_raw = item.css('div.col-gird-padding > div.gird-padding-time::text').get()
            month_year = month_year_raw.strip() if month_year_raw else ""
            
            # Ghép Ngày/Tháng/Năm
            full_date = f"{day} {month_year}"
            
            # Lấy Tiêu đề
            title = item.css('a.gird-padding-custom-title::text').get()
            title_clean = title.strip() if title else None
            
            # Lấy Đường dẫn (URL)
            url = item.css('a.gird-padding-custom-title::attr(href)').get()

            e_item = EventItem()
            e_item['mcp'] = 'DGT'
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title_clean
            e_item['details_raw'] = str(title_clean) +'\n' + str(url)
            e_item['date'] = convert_date_to_iso8601(full_date)               
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
