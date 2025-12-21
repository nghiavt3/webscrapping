import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_bms'
    mcpcty = 'BMS'
    # Thay thế bằng domain thực tế
    allowed_domains = ['bmsc.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://bmsc.com.vn/tin-co-dong/']

    def parse(self, response):
        for item in response.css('.viewcat_list .item'):
            # Trích xuất dữ liệu
            title = item.css('a::attr(title)').get()
            link = item.css('a::attr(href)').get()
            
            # Trích xuất ngày đăng (loại bỏ khoảng trắng thừa)
            # Sử dụng ::text để lấy text trực tiếp trong li sau thẻ em
            date_raw = item.css('.text-muted li:nth-child(1)::text').getall()
            date = "".join(date_raw).strip() if date_raw else None
            
            # Trích xuất lượt xem
            view_raw = item.css('.text-muted li:nth-child(2)::text').getall()
            views = "".join(view_raw).strip() if view_raw else None
                

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n'  + str(link)
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
    input_format = '%d/%m/%Y %I:%M:%S %p'
    
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
