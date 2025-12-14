import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_ddv'
    mcpcty= 'DDV'
    # Thay thế bằng domain thực tế
    allowed_domains = ['dap-vinachem.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.dap-vinachem.com.vn/thong-bao-tin-tuc'] 

    def parse(self, response):
        # Chọn tất cả các item tin tức
        news_items = response.css('.list-news.row .item-wrap')

        for item in news_items:
            # Trích xuất dữ liệu cho từng bài tin
            title = item.css('h5.item-title a::text').get().strip()
            url = item.css('h5.item-title a::attr(href)').get()
            
            # Trích xuất data-src cho ảnh
            image_src = item.css('img::attr(data-src)').get()
            
            # Trích xuất ngày đăng (lưu ý: .date có 2 thẻ, nên ta chọn thẻ nằm trong item-body)
            date_raw = item.css('.item-body .date::text').get()
            # Xử lý chuỗi để lấy ngày, ví dụ: ' 30/07/2025' -> '30/07/2025'
            date = date_raw.replace('\r', '').replace('\n', '').strip() if date_raw else None
            
            # Trích xuất tóm tắt
            summary = item.css('p.content::text').get().strip()

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(summary) +'\n' +str(url)
            e_item['date'] = convert_date_to_iso8601(date)               
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
