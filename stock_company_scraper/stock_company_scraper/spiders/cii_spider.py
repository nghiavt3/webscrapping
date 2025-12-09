import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_cii'
    # Thay thế bằng domain thực tế
    allowed_domains = ['cii.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://cii.com.vn/category/thong-tin-cong-bo'] 

    def parse(self, response):
        for item in response.css('div.post_item'):
            e_item = EventItem()


            # 2. Trích xuất thuộc tính 'year' (năm)
            current_year = str(datetime.now().year)

            # Trích xuất dữ liệu
            date_raw = item.css('div.date_post span::text').get()
            title = item.css('div.title_post h3 a::text').get()
            detail_url = item.css('div.title_post h3 a::attr(href)').get()
            summary = item.css('div.excerpt_post p span::text').get()
            
            # Làm sạch dữ liệu
            cleaned_date = date_raw.strip() if date_raw else None
            cleaned_title = title.strip() if title else None
            cleaned_summary = summary.strip() if summary else None

            e_item['mcp'] = 'CII'
            e_item['web_source'] = 'cii.com.vn'
            e_item['summary'] = cleaned_title
            e_item['details_raw'] = cleaned_title +'\n' + cleaned_summary + '\n' + detail_url
            e_item['date'] = convert_viet_date_to_iso8601(cleaned_date,current_year)               
            yield e_item

from datetime import datetime

MONTH_MAPPING = {
    'Tháng 01': '01', 'Tháng 1': '01',
    'Tháng 02': '02', 'Tháng 2': '02',
    'Tháng 03': '03', 'Tháng 3': '03',
    'Tháng 04': '04', 'Tháng 4': '04',
    'Tháng 05': '05', 'Tháng 5': '05',
    'Tháng 06': '06', 'Tháng 6': '06',
    'Tháng 07': '07', 'Tháng 7': '07',
    'Tháng 08': '08', 'Tháng 8': '08',
    'Tháng 09': '09', 'Tháng 9': '09',
    'Tháng 10': '10',
    'Tháng 11': '11',
    'Tháng 12': '12'
}

def convert_viet_date_to_iso8601(vietnam_day_month_str, year):
    """
    Chuyển đổi chuỗi ngày/tháng tiếng Việt (DD Tháng MM) và năm sang 'YYYY-MM-DD'.
    
    :param vietnam_day_month_str: Chuỗi ngày/tháng, ví dụ: '28 Tháng 11'.
    :param year: Chuỗi hoặc số năm, ví dụ: 2025.
    :return: Chuỗi ngày tháng ISO 8601, ví dụ: '2025-11-28', hoặc None nếu có lỗi.
    """
    if not vietnam_day_month_str or not year:
        return None

    # Ví dụ: '28 Tháng 11' -> ['28', 'Tháng 11']
    parts = vietnam_day_month_str.strip().split(maxsplit=1)
    
    if len(parts) < 2:
        return None
    
    day = parts[0]
    month_raw = parts[1]
    
    # 1. Ánh xạ tên tháng sang số
    month = MONTH_MAPPING.get(month_raw.strip(), None)
    
    if not month:
        print(f"⚠️ Lỗi: Không tìm thấy số tháng cho '{month_raw}'")
        return None
        
    # 2. Tạo chuỗi ngày tháng đầy đủ (DD/MM/YYYY)
    full_date_str = f"{day}/{month}/{year}"
    
    try:
        # 3. Parse và Định dạng lại
        date_object = datetime.strptime(full_date_str, '%d/%m/%Y')
        iso_date_str = date_object.strftime('%Y-%m-%d')
        
        return iso_date_str
    
    except ValueError as e:
        print(f"⚠️ Lỗi chuyển đổi ngày tháng '{full_date_str}': {e}")
        return None
