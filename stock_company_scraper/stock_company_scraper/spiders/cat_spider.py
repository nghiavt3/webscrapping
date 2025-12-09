import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_cat'
    # Thay thế bằng domain thực tế
    allowed_domains = ['seaprimexco.com'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://seaprimexco.com/vi/shareholder_relation'] 

    def parse(self, response):
        # 1. Định vị bảng chính xác bằng ID
        # Sử dụng CSS Selector để chọn thẻ <tbody> của bảng có ID là 'event-formdata'
        news_selectors = response.css('div.news-item-list')
        
        if not news_selectors:
            self.logger.warning("Không tìm thấy bảng dữ liệu với class 'div.news-item-list'")
            return

        
        for selector in news_selectors:
            # --- Khởi tạo Item (Giả định bạn có NewsItem được định nghĩa)
            item = EventItem()
            
            # --- 1. Trích xuất Tiêu đề (Title) ---
            # Tiêu đề nằm trong thẻ <h3>. Selector cần tính đến cả 2 cấu trúc.
            # Cấu trúc lớn: .news-content h3 ::text
            # Cấu trúc nhỏ: .item h3 ::text
            title = selector.css('h3::text').get()
            
            # --- 2. Trích xuất Ngày (Date) ---
            # Ngày nằm trong thẻ <p> và được in đậm trong thẻ <strong>
            date_raw = selector.css('p strong::text').get()
            # CHUYỂN ĐỔI DỮ LIỆU NGAY TẠI ĐÂY
            formatted_date = convert_vietnamese_date(date_raw)
            # --- 3. Trích xuất URL Tải xuống (Download URL) ---
            # URL nằm trong thuộc tính 'href' của thẻ <a> bên trong div.btn-haisan
            download_url = selector.css('div.btn-haisan a::attr(href)').get()
            
            # --- 4. Làm sạch và Gán vào Item ---
            if title and download_url:
                item['mcp'] = 'CAT'
                item['web_source'] = 'seaprimexco.com'
                item['summary'] = title.strip()
                item['date'] = formatted_date if formatted_date else None
                item['details_raw'] = title.strip() + '\n' + download_url
                #item['details_clean'] = download_url
                #item['download_url'] = download_url
                yield item

    

def convert_vietnamese_date(date_str):
    if not date_str:
        return None

    # Bước 1: Chuẩn hóa chuỗi (thay thế tất cả các loại khoảng trắng bằng một khoảng trắng đơn)
    date_str = date_str.strip().lower()
    
    # Sử dụng Regex để thay thế bất kỳ chuỗi khoảng trắng nào (bao gồm cả tab, newlines, etc.) 
    # bằng một khoảng trắng duy nhất, và loại bỏ các số 0 thừa trước tháng nếu có.
    date_str_clean = re.sub(r'\s+', ' ', date_str) 

    # Bước 2: Thay thế "thX" bằng số tháng
    month_mapping = {
        'th1': '1', 'th2': '2', 'th3': '3', 'th4': '4', 
        'th5': '5', 'th6': '6', 'th7': '7', 'th8': '8', 
        'th9': '9', 'th10': '10', 'th11': '11', 'th12': '12'
    }
    
    found_month = False
    for vn_month, num_month in month_mapping.items():
        if vn_month in date_str_clean:
            date_str_clean = date_str_clean.replace(vn_month, num_month)
            found_month = True
            break
    
    if not found_month:
        print(f"DEBUG: Không tìm thấy định dạng tháng tiếng Việt hợp lệ trong '{date_str}'")
        return None

    # Bước 3: Loại bỏ ký tự '0' thừa ở đầu tháng số (Ví dụ: '1 012 2025' -> '1 12 2025')
    # Thường xảy ra khi tháng được trích xuất là '01', '02', v.v.
    parts = date_str_clean.split()
    if len(parts) == 3:
        # Nếu tháng bắt đầu bằng '0' và có độ dài > 1, loại bỏ '0'
        if len(parts[1]) > 1 and parts[1].startswith('0'):
             parts[1] = parts[1].lstrip('0')
             
        date_str_clean = ' '.join(parts)


    # Bước 4: Phân tích cú pháp chuỗi đã được chuẩn hóa lại
    try:
        # Định dạng mong muốn sau khi chuẩn hóa: "28 11 2025"
        date_object = datetime.strptime(date_str_clean.strip(), "%d %m %Y")
        
        # Chuyển đổi sang định dạng chuỗi ISO 8601 chuẩn cho SQLite
        return date_object.strftime("%Y-%m-%d %H:%M:%S")

    except ValueError as e:
        # Lỗi sẽ xảy ra nếu chuỗi cuối cùng không khớp định dạng
        print(f"Lỗi chuyển đổi ngày tháng cuối cùng '{date_str_clean}': {e}")
        return None
