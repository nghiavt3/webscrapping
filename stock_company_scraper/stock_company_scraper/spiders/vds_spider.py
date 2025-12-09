import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_vds'
    # Thay thế bằng domain thực tế
    allowed_domains = ['vdsc.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://vdsc.com.vn/quan-he-co-dong/cong-bo-thong-tin'] 

    def parse(self, response):

        items = response.css('div.news-4 > div.col-md-3')
        
        for item in items:
            
            # Trích xuất dữ liệu
            date_full = item.css('a.item .text-content span::text').get()
            title = item.css('a.item h6.title::text').get()
            detail_url = item.css('a.item::attr(href)').get()
           # views = item.css('a.item .text-content span:nth-child(3)::text').get(default='')
            
            # Làm sạch dữ liệu
            cleaned_title = title
            cleaned_date = date_full
           # cleaned_views = views.strip() 

        
            e_item = EventItem()
            e_item['mcp'] = 'VDS'
            e_item['web_source'] = 'vdsc.com.vn'
            e_item['summary'] = cleaned_title
            e_item['details_raw'] =cleaned_title +'\n' + detail_url
            e_item['date'] = convert_date_dash_to_iso8601(cleaned_date)               
            yield e_item

from datetime import datetime

def convert_date_dash_to_iso8601(vietnam_date_str):
    """
    Chuyển đổi chuỗi ngày tháng từ định dạng 'DD-MM-YYYY' sang 'YYYY-MM-DD' (ISO 8601).
    
    :param vietnam_date_str: Chuỗi ngày tháng đầu vào, ví dụ: '28-10-2025'
    :return: Chuỗi ngày tháng ISO 8601, ví dụ: '2025-10-28', hoặc None nếu có lỗi.
    """
    if not vietnam_date_str:
        return None

    # Định dạng đầu vào: Ngày-Tháng-Năm ('%d-%m-%Y')
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
        print(f"⚠️ Lỗi chuyển đổi ngày tháng '{vietnam_date_str}' (phải là DD-MM-YYYY): {e}")
        return None