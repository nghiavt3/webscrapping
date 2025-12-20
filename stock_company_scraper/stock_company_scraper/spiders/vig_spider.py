import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_vig'
    mcpcty = 'VIG'
    # Thay thế bằng domain thực tế
    allowed_domains = ['visc.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://visc.com.vn/vi/news/cong-bo-thong-tin-3103.spp'] 

    def parse(self, response):
        # Chọn các thẻ <a> là con trực tiếp của khối danh sách
        articles = response.css('#ctl00_pnlArticleList > a')

        for art in articles:
            # Lấy ngày từ thuộc tính data-date (thường chính xác hơn text hiển thị)
            raw_date = art.css('::attr(data-date)').get()
            title = (art.css('h2::text').get() or "").strip()
            link = (art.css('::attr(href)').get() or "").strip()
            

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n'+ str(link)
            e_item['date'] = convert_date_to_iso8601(raw_date)               
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
    clean_date = vietnam_date_str.split(' ')[0].strip()
    # Định dạng đầu vào: Ngày/Tháng/Năm ('%d/%m/%Y')
    input_format = '%d/%m/%Y'
    
    # Định dạng đầu ra: Năm-Tháng-Ngày ('%Y-%m-%d') - chuẩn ISO 8601 cho ngày
    output_format = '%Y-%m-%d'

    try:
        # 1. Parse chuỗi đầu vào thành đối tượng datetime
        date_object = datetime.strptime(clean_date.strip(), input_format)
        
        # 2. Định dạng lại đối tượng datetime thành chuỗi ISO 8601
        iso_date_str = date_object.strftime(output_format)
        
        return iso_date_str
    
    except ValueError as e:
        print(f"⚠️ Lỗi chuyển đổi ngày tháng '{vietnam_date_str}' (phải là DD/MM/YYYY): {e}")
        return None
