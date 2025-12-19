import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_shi'
    mcpcty = 'SHI'
    # Thay thế bằng domain thực tế
    allowed_domains = ['sonha.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.sonha.com.vn/thong-tin-cong-bo/'] 

    def start_requests(self):
        yield scrapy.Request(
        url=self.start_urls[0],
        callback=self.parse,
        # Thêm meta để kích hoạt Playwright
        meta={'playwright': True}
    )
        
    def parse(self, response):
        # 1. Trích xuất danh sách các Năm (Tùy chọn)
        years = response.css('div.list-tab-year ul li a::text').getall()
        self.logger.info(f"Các năm có sẵn: {years}")

        # 2. Lặp qua từng hàng dữ liệu trong bảng (mỗi hàng là một thông báo)
        rows = response.css('div.list-shareholder table tbody tr')
        
        for row in rows:
            # 3. Trích xuất từng cột dữ liệu:
            
            # Cột 1: Ngày đăng (ví dụ: '04/12/2025')
            date_posted = row.css('td:nth-child(1)::text').get().strip()
            
            # Cột 2: Tên tài liệu (ví dụ: 'CBTT_ Thông báo giải thể...')
            document_name = row.css('td:nth-child(2)::text').get().strip()
            
            # Cột 3: Link Tải về (Lấy thuộc tính href của thẻ <a>)
            download_url = row.css('td:nth-child(3) a::attr(href)').get()

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = document_name
            e_item['details_raw'] = str(document_name) +'\n' + str(download_url)
            e_item['date'] = convert_date_to_iso8601(date_posted)               
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
