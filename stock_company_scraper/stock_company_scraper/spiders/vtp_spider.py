import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_vtp'
    # Thay thế bằng domain thực tế
    allowed_domains = ['viettelpost.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://viettelpost.com.vn/tin-co-dong/'] 
    # Ghi đè cấu hình CHỈ CHO SPIDER NÀY
    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
    }

    def start_requests(self):
        yield scrapy.Request(
        url=self.start_urls[0],
        callback=self.parse,
        # Thêm meta để kích hoạt Playwright
        meta={'playwright': True}
    )
    
    def parse(self, response):
        records = response.css('div.first-item, div.second-item, div.normal-item')
        print(records)
        for record in records:
            
            # Tiêu đề: Lấy từ h5 (tin nổi bật) hoặc p.title (tin thường)
            title = record.css('h5::text, p.title::text').get()
            
            # URL Bài viết: Thẻ <a> đầu tiên trong item chứa link bài viết
            article_url_relative = record.css('a::attr(href)').get()
            
            # Ngày: Lấy từ span trong div.meta (tin nổi bật) hoặc p.date (tin thường)
            date_nodes = record.css('div.meta span::text, p.date::text').getall()
            date_raw = " ".join(date_nodes).strip()
            #date_raw = record.css('div.meta span::text, p.date::text').get()
            
            # Mô tả: Lấy từ p.des (nổi bật) hoặc p.description (thường)
            description = record.css('p.des::text, p.description::text').get()
            
            # Hình ảnh: Lấy từ img.thumb (nổi bật) hoặc img trong div.box-img (thường)
            image_url_relative = record.css('img.thumb::attr(src), div.box-img img::attr(src)').get()
            
            # Làm sạch dữ liệu và chuẩn hóa URL
            cleaned_title = title.strip() if title else None
            # Loại bỏ các ký tự icon và khoảng trắng thừa
            cleaned_date = date_raw.replace('\xa0', '').strip() if date_raw else None 
            cleaned_description = description.strip() if description else None

            e_item = EventItem()
            e_item['mcp'] = 'VTP'
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_title
            e_item['details_raw'] = cleaned_title +'\n' + cleaned_description+ '\n' + article_url_relative
            e_item['date'] = convert_date_to_iso(cleaned_date)               
            yield e_item

from datetime import datetime
# Ánh xạ tên tháng Tiếng Việt sang số tháng
THANG_MAPPING = {
    'Tháng 1': 1, 'Tháng 2': 2, 'Tháng 3': 3, 'Tháng 4': 4,
    'Tháng 5': 5, 'Tháng 6': 6, 'Tháng 7': 7, 'Tháng 8': 8,
    'Tháng 9': 9, 'Tháng 10': 10, 'Tháng 11': 11, 'Tháng 12': 12,
}

def convert_date_to_iso(date_str):
    """
    Chuyển đổi chuỗi ngày tháng (21/10/25 HOẶC 15 Tháng 9, 2025) 
    sang định dạng ISO 8601 (YYYY-MM-DD).
    """
    if not date_str:
        return None

    date_str = date_str.strip()

    # 1. Xử lý định dạng "DD/MM/YY" (Ví dụ: 21/10/25)
    if '/' in date_str:
        try:
            # "%y" là năm hai chữ số (25 -> 2025). 
            # Giả định đây là ngày tháng Tiếng Việt (DD/MM/YY)
            date_obj = datetime.strptime(date_str, '%d/%m/%y')
            return date_obj.strftime('%Y-%m-%d')
        except ValueError:
            # Bỏ qua và chuyển sang xử lý định dạng khác
            pass

    # 2. Xử lý định dạng "Ngày Tháng X, Năm Y" (Ví dụ: 15 Tháng 9, 2025)
    elif 'Tháng' in date_str:
        try:
            parts = date_str.split(', ')
            day_month_parts = parts[0].split(' ', 1)
            
            day = int(day_month_parts[0].strip())
            month_name = day_month_parts[1].strip()
            year = int(parts[1].strip())
            
            month = THANG_MAPPING.get(month_name)
            
            if month is None:
                raise ValueError(f"Tên tháng không hợp lệ: {month_name}")

            date_obj = datetime(year, month, day)
            return date_obj.strftime('%Y-%m-%d')
            
        except Exception as e:
            print(f"Lỗi chuyển đổi ngày tháng Tiếng Việt '{date_str}': {e}")
            return None
            
    # Nếu không khớp định dạng nào
    print(f"Định dạng ngày tháng không xác định: {date_str}")
    return None