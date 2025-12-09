import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_mzg'
    # Thay thế bằng domain thực tế
    allowed_domains = ['miza.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://miza.vn/vi/quan-he-co-dong/cong-bo-thong-tin'] 

    def parse(self, response):
        # 1. Lấy danh sách tất cả các thông báo (records)
        # Trích xuất tất cả các thông báo riêng lẻ
        list_items = response.css('div.border.border-gray-200.rounded-lg.p-4')

        results = []

        for item in list_items:
            # 1. Trích xuất Tiêu đề (Title)
            # Thẻ h4 chứa class text-lg.font-semibold
            title = item.css('h4.text-lg.font-semibold::text').get()
            
            # 2. Trích xuất Tóm tắt (Summary/Details)
            # Thẻ p chứa class text-gray-600.text-sm
            # Lưu ý: Một số thông báo có thể không có tóm tắt (text trống)
            summary = item.css('p.text-gray-600.text-sm::text').get()
            
            # 3. Trích xuất Ngày (Date)
            # Thẻ span chứa ngày nằm trong div.flex.items-center.gap-1
            # Ta chọn span nằm ngay sau thẻ svg (hoặc chỉ chọn span)
            date_raw = item.css('div.flex.items-center.gap-4 span::text').get()
            
            # Làm sạch dữ liệu (loại bỏ khoảng trắng thừa)
            title_cleaned = title.strip() if title else None
            summary_cleaned = summary.strip() if summary else 'Không có tóm tắt.'
            date_cleaned = date_raw.strip() if date_raw else None

            e_item = EventItem()
            e_item['mcp'] = 'MZG'
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title_cleaned
            e_item['details_raw'] = title_cleaned +'\n' + summary_cleaned
            e_item['date'] = convert_date_to_iso8601(date_cleaned)               
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
