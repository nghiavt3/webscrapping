import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_hax'
    mcpcty = 'HAX'
    # Thay thế bằng domain thực tế
    allowed_domains = ['haxaco.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.haxaco.com.vn/dai-hoi-dong-co-dong/2025-98/'] 

    # def start_requests(self):
    #     yield scrapy.Request(
    #     url=self.start_urls[0],
    #     callback=self.parse,
    #     # Thêm meta để kích hoạt Playwright
    #     meta={'playwright': True}
    # )
        
    def parse(self, response):
        # Chọn tất cả các dòng trong thân bảng
        rows = response.css('div.main.newdautu table tbody tr')
        
        for row in rows:
            # 1. Trích xuất tiêu đề
            title = row.css('td:nth-child(1)::text').get()
            
            # 2. Trích xuất và làm sạch ngày tháng (loại bỏ phần giờ)
            raw_date_list = row.css('td:nth-child(2)::text').getall()
            # raw_date_list thường có dạng ['02/07/2025', '11:05:34']
            date_str = raw_date_list[0].strip() if raw_date_list else None
            
            iso_date = None
            if date_str:
                try:
                    iso_date = datetime.strptime(date_str, '%d/%m/%Y').strftime('%Y-%m-%d')
                except ValueError:
                    iso_date = None

            # 3. Trích xuất Link từ chuỗi Javascript onclick
            # Dùng Regex để tìm chuỗi nằm giữa dấu nháy đơn trong hàm downloadfile('...')
            onclick_text = row.css('td:nth-child(4) a::attr(onclick)').get()
            pdf_link = None
            if onclick_text:
                match = re.search(r"'(https://[^']+)'", onclick_text)
                if match:
                    pdf_link = match.group(1)

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title.strip()
            e_item['details_raw'] = str(title.strip()) +'\n' + str(pdf_link)
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
