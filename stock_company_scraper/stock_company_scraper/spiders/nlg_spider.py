import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_nlg'
    mcpcty = 'NLG'
    # Thay thế bằng domain thực tế
    allowed_domains = ['namlongvn.com'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.namlongvn.com/quan-he-nha-dau-tu/'] 

    def parse(self, response):
            # Chọn tất cả các khối tài liệu riêng lẻ trong khu vực "info-disclosure-section"
        doc_items = response.css('#info-disclosure-section .doc-item')

        for item in doc_items:
            # 1. Trích xuất Tiêu đề (lấy text)
            title = item.css('.doc-title a::text').get().strip() if item.css('.doc-title a::text').get() else None

            # 2. Trích xuất Đường link (lấy attribute href)
            # Dùng .doc-title a để đảm bảo lấy link của tài liệu, không phải link PDF nhỏ hơn
            link = item.css('.doc-title a::attr(href)').get()

            # 3. Trích xuất Ngày giờ (tùy chọn)
            # Lấy text trong div.datetime-label
            datetime_label = item.css('.doc-meta .datetime-label::text').get()
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(link)
            e_item['date'] = convert_date_to_iso8601(datetime_label)
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
    input_format = "%d/%m/%Y | %H:%M"    
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
