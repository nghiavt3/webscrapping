import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_gvr'
    mcpcty = 'GVR'
    # Thay thế bằng domain thực tế
    allowed_domains = ['rubbergroup.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://rubbergroup.vn/quan-he-co-dong/cong-bo-thong-tin'] 

    def start_requests(self):
        yield scrapy.Request(
        url=self.start_urls[0],
        callback=self.parse,
        # Thêm meta để kích hoạt Playwright
        meta={'playwright': True}
    )
        
    def parse(self, response):
        rows = response.css('table.tbl > tbody > tr[class^="file"]')
        
        for row in rows:
            # 1. Trích xuất STT
            # td:nth-child(1) là cột STT, dùng ::text để lấy nội dung text. 
            # Dùng .strip() để loại bỏ khoảng trắng thừa.
            stt = row.css('td:nth-child(1) strong::text').get().strip()

            # 2. Trích xuất Chủ đề và Ngày
            # Cột Chủ đề là td:nth-child(2)
            # Title: nằm trong <p> và <section>
            title = row.css('td:nth-child(2) section p::text').get().strip()
            # Date: nằm trong <span> có class "date2"
            date = row.css('td:nth-child(2) span.date2::text').get().strip('()') # Bỏ dấu ngoặc ()
            
            # 3. Trích xuất Liên kết Tải về và Kích thước
            # Cột Tải về là td:nth-child(3)
            # URL: Lấy thuộc tính 'href' của thẻ <a>
            download_url = row.css('td:nth-child(3) a::attr(href)').get()
            # Size: nằm trong <span> có class "span_size"
            download_size = row.css('td:nth-child(3) span.span_size::text').get().strip()

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(download_url)
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
