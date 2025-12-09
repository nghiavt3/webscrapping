import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_szc'
    mcpcty = 'SZC'
    # Thay thế bằng domain thực tế
    allowed_domains = ['sonadezichauduc.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://sonadezichauduc.com.vn/vn/thong-tin-co-dong.html'] 

    def parse(self, response):
        # 1. Chọn tất cả các mục tin tức
        items = response.css('div.list_download div.item')

        # 2. Lặp qua từng mục để trích xuất dữ liệu
        for item in items:
            # Trích xuất dữ liệu
            title = item.css('div.i-title a::text').get()
            article_url = item.css('div.i-title a::attr(href)').get()
            date_time_list = item.css('div.i-date ::text').getall()

            # Lọc và nối chuỗi trong Python
            if date_time_list:
                # 1. Loại bỏ các chuỗi rỗng và khoảng trắng
                clean_text_list = [t.strip() for t in date_time_list if t.strip()]
                
                # 2. Chuỗi cuối cùng bạn cần luôn là phần tử cuối cùng trong list đã lọc
                # Ví dụ: clean_text_list = ['2025-11-26', '26/11/2025, 16:07 PM']
                
                date_time_raw = clean_text_list[-1] if clean_text_list else None
                
                # 3. Chỉ lấy phần ngày
                if date_time_raw:
                    date_only = date_time_raw.split(',')[0]

            image_url = item.css('div.i-img img::attr(src)').get()
            download_url = item.css('div.link_download a::attr(href)').get()

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = f"{str(title)}\n{str(article_url)}\n{str(download_url)}"
            e_item['date'] = convert_date_to_iso8601(date_only)
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
