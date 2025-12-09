import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_qcg'
    # Thay thế bằng domain thực tế
    allowed_domains = ['quoccuonggialai.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://quoccuonggialai.com.vn/thong-tin-co-dong/'] 

    def parse(self, response):
        records = response.css('.itemList div.MTContainer:not(:first-child)')
        
        for record in records:
            
            # Trích xuất dữ liệu
            title_raw = record.css('h2.MTItemTitle a::text').get()
            doc_url_raw ='https://quoccuonggialai.com.vn'+ record.css('h2.MTItemTitle a::attr(href)').get()
            date_created = record.css('div.MTItemDateCreated::text').get()
            
            # URL tải về
            download_url_raw ='https://quoccuonggialai.com.vn' + record.css('div.MTItemDownload a::attr(href)').get()
            
            # Làm sạch dữ liệu
            cleaned_title = title_raw.strip() if title_raw else None
            cleaned_date = date_created.strip() if date_created else None

            e_item = EventItem()
            e_item['mcp'] = 'QCG'
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_title
            e_item['details_raw'] = cleaned_title +'\n' + doc_url_raw+ '\n' + download_url_raw
            e_item['date'] = convert_date_to_iso8601(cleaned_date)               
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
