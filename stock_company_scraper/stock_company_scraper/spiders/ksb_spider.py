import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_ksb'
    mcpcty = 'KSB'
    # Thay thế bằng domain thực tế
    allowed_domains = ['ksb.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://ksb.vn/quan-he-co-dong/'] 

    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
    }

    def start_requests(self):
        yield scrapy.Request(
        url=self.start_urls[0],
        callback=self.parse,
        # Thêm meta để kích hoạt Playwright
        meta={'playwright': True}
    )
        
    def parse(self, response):
        rows = response.css('tbody tr') 

        for row in rows:
            # 1. Trích xuất Ngày tháng (Date)
            # Lấy ngày (span đầu tiên) và tháng-năm (span thứ hai) và kết hợp chúng
            day = row.css('td.time div.date span:nth-child(1)::text').get().strip()
            month_year_raw = row.css('td.time div.date span:nth-child(2)::text').get().strip() # Ví dụ: '10-2025'

            # Định dạng lại ngày tháng
            # Kết hợp Day, Month, Year thành một chuỗi: "24/10/2025"
            date_formatted = f"{day}/{month_year_raw.replace('-', '/')}"

            # 2. Trích xuất Tiêu đề (Title)
            # Chọn thẻ <a> bên trong td.name và lấy văn bản
            title = row.css('td.name h4.title a::text').get().strip()

            # 3. Trích xuất URL Chi tiết (Detail URL)
            # Chọn thẻ <a> trong td.detail và lấy thuộc tính href
            detail_url = row.css('td.detail a::attr(href)').get()
            
            # 4. Trích xuất URL Tải file (Download URL)
            # Chọn thẻ <a> trong td.down và lấy thuộc tính href
            download_url = row.css('td.down a::attr(href)').get()

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(detail_url) +'\n' + str(download_url)
            e_item['date'] = convert_date_to_iso8601(date_formatted)               
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
