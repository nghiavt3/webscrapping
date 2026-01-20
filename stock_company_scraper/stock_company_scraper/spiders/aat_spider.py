import scrapy
import sqlite3
import os
from stock_company_scraper.items import EventItem
from datetime import datetime
import re

class EventSpider(scrapy.Spider):
    name = 'event_aat'
    mcpcty = 'AAT'
    allowed_domains = ['tiensonaus.com'] 
    start_urls = ['https://tiensonaus.com/quan-he-co-dong/cong-bo-thong-tin/'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        # Đường dẫn file db
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # Mở kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Đảm bảo bảng tồn tại (Sử dụng cấu trúc chuẩn bạn đã sửa)
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY,
                mcp TEXT,
                date TEXT,
                summary TEXT,
                scraped_at TEXT,
                web_source TEXT,
                details_clean TEXT
            )
        ''')

        # Lặp qua danh sách bài viết
        articles = response.css('article.item-list')
        
        for article in articles:
            # 1. Trích xuất dữ liệu cơ bản
            title = article.css('h2.post-box-title a::text').get()
            link = article.css('h2.post-box-title a::attr(href)').get()
            raw_date = article.css('div.css_link02::text').get()
            
            # Làm sạch dữ liệu
            title = title.strip() if title else ""
            clean_date = raw_date.strip() if raw_date else ""
            iso_date = convert_date_to_iso8601(clean_date)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Tạo ID theo quy tắc chuẩn để so khớp với Database
            event_id = f"{title}_{iso_date}".replace('/', '-').replace('.', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT GIA TĂNG.")
                conn.close()
                break # THOÁT NGAY LẬP TỨC

            # 4. Đưa dữ liệu vào Item nếu là tin mới
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = f"{title}\nLink: {link}"
            e_item['date'] = iso_date 
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    """
    Chuyển đổi từ 'DD/MM/YYYY HH:MM:SS' sang 'YYYY-MM-DD'
    """
    if not vietnam_date_str:
        return None

    # Lưu ý: định dạng của AAT có cả giờ phút giây
    input_format = '%d/%m/%Y %H:%M:%S'
    output_format = '%Y-%m-%d'

    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        iso_date_str = date_object.strftime(output_format)
        return iso_date_str
    except ValueError:
        # Thử lại với định dạng không có giờ nếu lỗi (phòng hờ dữ liệu web thay đổi)
        try:
            date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y')
            return date_object.strftime(output_format)
        except:
            return None