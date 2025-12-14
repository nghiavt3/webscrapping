import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_pdr'
    # Thay thế bằng domain thực tế
    allowed_domains = ['phatdat.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.phatdat.com.vn/thong-bao-co-dong/'] 

    def parse(self, response):
        # 1. Lấy danh sách tất cả các thông báo (records)
        records = response.css('div.block-record')
        
        for record in records:
            
            # Trích xuất dữ liệu
            # Lấy phần tử thứ 2 (index 1) của text nodes để loại bỏ chuỗi "Ngày ban hành"
            date_raw = record.css('span.block-cell.flex-center::text').getall()[1]
            
            title_element = record.css('span.block-cell a')
            title = title_element.css('::text').get()
            url = title_element.css('::attr(href)').get()
            
            # URL tải về nằm trong span cuối cùng, là thẻ <a> duy nhất trong span đó
            download_url = record.css('span.block-cell.flex-center a::attr(href)').get()
            
            # Làm sạch dữ liệu
            cleaned_date = date_raw.strip() if date_raw else None
            cleaned_title = title.strip() if title else None
            e_item = EventItem()
            e_item['mcp'] = 'PDR'
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_title
            e_item['details_raw'] = str(cleaned_title) +'\n' + str(url)+ '\n' + str(download_url)
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
