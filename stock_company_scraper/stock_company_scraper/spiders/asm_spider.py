import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_asm'
    mcpcty = 'ASM'
    # Thay thế bằng domain thực tế
    allowed_domains = ['saomaigroup.com'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://saomaigroup.com/vn/cong-bo-thong-tin.html'] 

    # def start_requests(self):
    #     yield scrapy.Request(
    #     url=self.start_urls[0],
    #     callback=self.parse,
    #     # Thêm meta để kích hoạt Playwright
    #     meta={'playwright': True}
    # )
        
    def parse(self, response):
        # Duyệt qua từng dòng tin tức
        for item in response.css('div.itholdermm'):
            # Lấy dữ liệu thô
            raw_title = item.css('div.mmtitle a::text').get()
            raw_date = item.css('div.mmdate::text').get()
            raw_link = item.css('div.mmtitle a::attr(href)').get()

            # Xử lý chuyển đổi ngày tháng (Ví dụ: 29.11.2025 -> 2025-11-29)
            iso_date = None
            if raw_date:
                try:
                    # Đảm bảo raw_date là chuỗi (tránh lỗi tuple)
                    date_str = raw_date[0] if isinstance(raw_date, (list, tuple)) else raw_date
                    # Loại bỏ khoảng trắng và làm sạch
                    clean_date = date_str.strip()
                    # Parse với định dạng dấu chấm '.'
                    date_obj = datetime.strptime(clean_date, '%d.%m.%Y')
                    iso_date = date_obj.strftime('%Y-%m-%d')
                except (ValueError, AttributeError):
                    iso_date = None

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = raw_title.strip()
            e_item['details_raw'] = str(raw_title.strip()) +'\n' + str(raw_link)
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
