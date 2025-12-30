import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_vib'
    mcpcty= 'VIB'

    # Thay thế bằng domain thực tế
    allowed_domains = ['vib.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    #start_urls = ['https://www.vib.com.vn/vn/nha-dau-tu/cong-bo-thong-tin','https://www.vib.com.vn/vn/nha-dau-tu/thong-tin-co-dong'] 

    # def start_requests(self):
    #     yield scrapy.Request(
    #     url=self.start_urls[0],
    #     callback=self.parse,
    #     # Thêm meta để kích hoạt Playwright
    #     meta={'playwright': True}
    # )
    
    def start_requests(self):
        urls = [
            ('https://www.vib.com.vn/vn/nha-dau-tu/cong-bo-thong-tin', self.parse_cong_bo),
            ('https://www.vib.com.vn/vn/nha-dau-tu/thong-tin-co-dong', self.parse_co_dong),
        ]
        for url, callback in urls:
            yield scrapy.Request(url=url, callback=callback,meta={'playwright': True})

    def parse_co_dong(self, response):
        # Chọn tất cả các thẻ h4 bên trong container chính
            posts = response.css('.vib-v2-report-tab-list-detail h4')
            
            for post in posts:
                # Trích xuất và làm sạch dữ liệu
                title = (post.css('a::text').get() or "").strip()
                link = post.css('a::attr(href)').get()
                date = post.css('i::text').get()
                e_item = EventItem()
                # 2. Trích xuất dữ liệu chi tiết
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['date'] = convert_date_to_iso8601(date)
                e_item['summary'] = title
                e_item['details_raw'] = str(title) + '\n' + str(link)         
                yield e_item    

    def parse_cong_bo(self, response):
        # Selector cho cấu trúc h4 bạn vừa gửi
            posts = response.css('.vib-v2-report-tab-list-detail h4')
            
            for post in posts:
                title = (post.css('a::text').get() or "").strip()
                link = post.css('a::attr(href)').get()
                date = post.css('i::text').get()
                e_item = EventItem()
                # 2. Trích xuất dữ liệu chi tiết
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['date'] = convert_date_to_iso8601(date)
                e_item['summary'] = title
                e_item['details_raw'] = str(title) + '\n' + str(link)         
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
    input_format = '%d/%m/%Y %H:%M'
    
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
