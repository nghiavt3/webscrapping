import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_vgs'
    mcpcty = 'VGS'
    # Thay thế bằng domain thực tế
    allowed_domains = ['vgpipe.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://vgpipe.com.vn/qhcd-dm/cong-bo-thong-tin/'] 

    # def start_requests(self):
    #     yield scrapy.Request(
    #     url=self.start_urls[0],
    #     callback=self.parse,
    #     # Thêm meta để kích hoạt Playwright
    #     meta={'playwright': True}
    # )
        
    def parse(self, response):
        
        for item in response.css('.qhch-list'):
            
            # 1. Lấy ngày gốc và chuyển sang ISO 8601 (YYYY-MM-DD)
            raw_date = item.css('.qhch-list-date span::text').get()
            iso_date = None
            if raw_date:
                try:
                    # Chuyển từ "09/12/2025" -> đối tượng datetime -> "2025-12-09"
                    iso_date = datetime.strptime(raw_date.strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
                except ValueError:
                    iso_date = raw_date # Trả về gốc nếu lỗi định dạng

            # 2. Lấy tiêu đề và làm sạch khoảng trắng
            title = item.css('.qhch-list-title a::text').get()
            if title:
                title = title.strip()

            # 3. Lấy link file PDF
            file_url = item.css('.qhch-list-title a::attr(href)').get()
            
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = str(title)
            e_item['details_raw'] = str(title) +'\n' + str(file_url)
            e_item['date'] = (iso_date)
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
