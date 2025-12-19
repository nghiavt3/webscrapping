import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
from scrapy_playwright.page import PageMethod

class EventSpider(scrapy.Spider):
    name = 'event_pom'
    mcpcty = 'POM'
    # Thay thế bằng domain thực tế
    allowed_domains = ['pomina-steel.com'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['http://www.pomina-steel.com/co-dong.html'] 
    # def start_requests(self):
    #     """Gửi request đến API với header thích hợp."""
    #     for url in self.start_urls:
    #         # Tùy chọn: Đặt header để mô phỏng một request từ trình duyệt/ứng dụng
    #         yield scrapy.Request(
    #             url=url,
    #             callback=self.parse,
    #             meta={
    #                 "playwright_page_methods": [
    #                     PageMethod("wait_for_selector", "div.relation-box .list-box:nth-child(1)", timeout=30000)
    #                 ]
    #             }
    #         )

    def parse(self, response):
        # 1. Lấy danh sách tất cả các thông báo (records)
        # Lặp qua từng khối tin tức
        for item in response.css('div.list-box'):
            title= item.css('div.r-text p::text').get()
            link= response.urljoin(item.css('a::attr(href)').get())
            # Lấy chuỗi ngày thô (ví dụ: 29-10-2025)
            raw_date = item.css('span.r-date::text').get()
            
            # Xử lý chuyển đổi ngày sang YYYY-MM-DD
            iso_date = None
            if raw_date:
                try:
                    # Định dạng gốc là Ngày-Tháng-Năm
                    date_obj = datetime.strptime(raw_date.strip(), '%d-%m-%Y')
                    iso_date = date_obj.strftime('%Y-%m-%d')
                except ValueError:
                    iso_date = raw_date

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(link)
            e_item['date'] = iso_date              
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
