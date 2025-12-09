import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_dha'
    mcpcty = 'DHA'
    # Thay thế bằng domain thực tế
    allowed_domains = ['hoaan.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['http://www.hoaan.com.vn/nam-2025-cn62/'] 

    def parse(self, response):
        # Chọn chính xác thẻ div.row thứ hai trong section.container
        second_row = response.css('section.container > div.row:nth-child(2)')
        
        if not second_row:
            self.log("Không tìm thấy thẻ div.row thứ 2.", level=scrapy.log.WARNING)
            return

        # --- 1. Trích xuất nhóm Bài viết chính/nổi bật ---
        main_articles = second_row.css('.investor-main, .investor-left')

        for article in main_articles:
            title = article.css('h4 a::text').get(default='').strip()
            relative_url = article.css('h4 a::attr(href)').get(default='')
            date = article.css('.info .date::text').get(default='').strip()

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = title +'\n' + relative_url
            e_item['date'] = date              
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
