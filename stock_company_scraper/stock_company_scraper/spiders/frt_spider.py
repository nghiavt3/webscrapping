import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_frt'
    mcpcty = 'FRT'
    # Thay thế bằng domain thực tế
    allowed_domains = ['frt.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://frt.vn/quan-he-co-dong'] 

    def start_requests(self):
        yield scrapy.Request(
        url=self.start_urls[0],
        callback=self.parse,
        # Thêm meta để kích hoạt Playwright
        meta={'playwright': True}
    )
        
    def parse(self, response):
        
            
            # Sử dụng selector linh hoạt theo tiền tố để tránh lỗi khi web đổi mã hash
        for item in response.css('div[class^="reports_file"]'):
            
            title = item.css('div[class^="reports_txt"]::text').get()
            pdf_url = item.css('a[class^="reports_title"]::attr(href)').get()
            
            # Xử lý trích xuất ngày tháng từ URL của FRT (thường có dạng 20251119_...)
            iso_date = None
            if pdf_url:
                # Tìm chuỗi 8 chữ số liên tiếp trong URL
                date_match = re.search(r'(\d{4})(\d{2})(\d{2})', pdf_url)
                if date_match:
                    year, month, day = date_match.groups()
                    iso_date = f"{year}-{month}-{day}"

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(pdf_url)
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
