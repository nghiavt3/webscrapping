import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_dse'
    mcpcty = 'DSE'
    # Thay thế bằng domain thực tế
    allowed_domains = ['ir.dnse.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://ir.dnse.com.vn/vi/ntag-cong-bo-thong-tin-16'] 

    def parse(self, response):
        # Lặp qua từng thẻ info-card
        for card in response.css('.info-card'):
            # 1. Lấy dữ liệu thô
            title = card.css('.info-title a::text').get()
            link = card.css('.info-title a::attr(href)').get()
            
            day = card.css('.highlight-day::text').get()
            month_year = card.css('.small-date::text').get() # Kết quả: "12 - 2025"

            # 2. Xử lý và chuyển đổi ngày tháng sang ISO 8601
            iso_date = None
            if day and month_year:
                try:
                    # Ghép chuỗi và làm sạch (loại bỏ khoảng trắng thừa)
                    # Kết quả mong muốn: "15/12/2025"
                    raw_date_str = f"{day.strip()}/{month_year.replace(' ', '').strip()}"
                    
                    # Chuyển thành object datetime
                    dt_obj = datetime.strptime(raw_date_str, '%d/%m-%Y')
                    
                    # Chuyển sang định dạng YYYY-MM-DD
                    iso_date = dt_obj.strftime('%Y-%m-%d')
                except Exception as e:
                    self.logger.error(f"Lỗi chuyển đổi ngày: {e}")
            

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(response.urljoin(link) if link else None)
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
