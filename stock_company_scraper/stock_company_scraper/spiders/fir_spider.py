import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_fir'
    mcpcty = 'FIR'
    # Thay thế bằng domain thực tế
    allowed_domains = ['fir.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://fir.vn/vn/quan-he-co-dong/cong-bo-thong-tin/'] 

    def start_requests(self):
        yield scrapy.Request(
        url=self.start_urls[0],
        callback=self.parse,
        # Thêm meta để kích hoạt Playwright
        meta={"playwright": True,
                
                }
    )
    def parse(self, response):
        # Duyệt qua từng item trong danh sách
        for item in response.css('div.report-list div.item'):
            # 1. Trích xuất ngày tháng thô
            raw_day = item.css('.number-card h5::text').get()          # "06"
            raw_month_year = item.css('.number-card p::text').get()    # "12-2025"
            
            # 2. Xử lý và chuyển đổi sang ISO (YYYY-MM-DD)
            # Kết hợp thành chuỗi "06-12-2025"
            full_date_str = f"{raw_day.strip()}-{raw_month_year.strip()}"
            try:
                # Chuyển từ DD-MM-YYYY sang đối tượng datetime
                date_obj = datetime.strptime(full_date_str, '%d-%m-%Y')
                iso_date = date_obj.strftime('%Y-%m-%d')
            except Exception:
                iso_date = None

            # 3. Trích xuất các thông tin khác
            title = item.css('.content a::text').get()
            file_url = item.css('.content a::attr(href)').get()

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = str(title.strip() if title else None)
            e_item['details_raw'] = str(title.strip() if title else None) +'\n' + str(response.urljoin(file_url) if file_url else None)
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
