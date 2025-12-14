import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_dig'
    mcpcty = 'DIG'
    # Thay thế bằng domain thực tế
    allowed_domains = ['dic.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.dic.vn/thong-tin-co-dong'] 

    def parse(self, response):
        news_items = response.css('#shareholders .item.col-md-6')
        
        for item in news_items:
            # 2. Tiêu đề thông báo
            # Chọn thẻ <a> có class "title" và lấy văn bản bên trong
            title = item.css('a.title::text').get().strip() if item.css('a.title::text').get() else None

            # 3. Đường dẫn chi tiết (Relative URL)
            # Chọn thẻ <a> bao ngoài có thuộc tính href.
            # Hoặc chọn thẻ <a> có class "more"
            relative_url = item.css('a::attr(href)').get()

            # 4. Ngày công bố
            # Chọn thẻ <span> ngay sau thẻ <i> có class "fa fa-calendar" 
            # Sử dụng selector XPath để chắc chắn lấy được nội dung trong thẻ <i>
            date_raw = item.css('.intro1 span *::text').get()
            
            # Xử lý chuỗi ngày (bỏ ký tự ' ' và '<i>')
            date_clean = date_raw.strip().replace('<i>', '').replace('</i>', '') if date_raw else None
            
            # Xây dựng URL đầy đủ (Absolute URL)
            # Nếu đường dẫn không bắt đầu bằng http(s)
            absolute_url = response.urljoin(relative_url) if relative_url else None

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) + '\n' + str(absolute_url)
            e_item['date'] = convert_date_to_iso8601(date_clean)
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
    input_format = "%d/%m/%Y"    
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
