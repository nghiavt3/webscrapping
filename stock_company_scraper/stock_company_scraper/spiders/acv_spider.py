import scrapy
import sqlite3
import os
from stock_company_scraper.items import EventItem
from datetime import datetime
import re

class EventSpider(scrapy.Spider):
    name = 'event_acv'
    allowed_domains = ['acv.vn'] 
    start_urls = ['https://acv.vn/tin-tuc/thong-bao-co-dong'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        # Đường dẫn file db khớp với cấu trúc dự án của bạn
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # Mở kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Đảm bảo bảng tồn tại (Cấu trúc chuẩn bạn đã hiệu chỉnh)
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

        notices = response.css('ul.blog-items li.item')
        
        for notice in notices:
            # 1. Trích xuất Tiêu đề và URL
            title_url_element = notice.css('h4.title a')
            title = title_url_element.css('::text').get().strip() if title_url_element else ""
            
            relative_url = title_url_element.css('::attr(href)').get()
            full_url = response.urljoin(relative_url) if relative_url else ""

            # 2. Trích xuất Thời gian và Ngày
            datetime_str = notice.css('div.datetime span::text').get()
            
            date_raw = None
            if datetime_str:
                parts = datetime_str.strip().split('|')
                if len(parts) == 2:
                    date_raw = parts[1].strip() # Lấy phần ngày (ví dụ: 04/12/2025)

            iso_date = convert_date_to_iso8601(date_raw)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Tạo ID chuẩn để so khớp với SQLite
            event_id = f"{title}_{iso_date}".replace('/', '-').replace('.', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT GIA TĂNG.")
                conn.close()
                break # THOÁT NGAY LẬP TỨC

            # 4. Đưa dữ liệu vào Item nếu là tin mới
            e_item = EventItem()
            e_item['mcp'] = 'ACV'
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = f"{title}\nLink: {full_url}"
            e_item['date'] = iso_date 
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    input_format = '%d/%m/%Y'
    output_format = '%Y-%m-%d'
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        return date_object.strftime(output_format)
    except ValueError as e:
        return None