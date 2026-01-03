import scrapy
import sqlite3
import os
from stock_company_scraper.items import EventItem
from datetime import datetime
import re

class EventSpider(scrapy.Spider):
    name = 'event_bcc'
    mcpcty = 'BCC'
    allowed_domains = ['ximangbimson.com.vn'] 
    start_urls = ['https://ximangbimson.com.vn/quan-he-co-dong/'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        # Đường dẫn file db đồng bộ với dự án
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # 1. Kết nối SQLite và chuẩn bị bảng
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
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

        # 2. Lấy danh sách các hàng trong bảng (bỏ qua hàng tiêu đề)
        rows = response.css('table tbody tr:not(:first-child)')

        for row in rows:
            # Trích xuất dữ liệu
            title = row.css('td:nth-child(2) a::text').get(default='').strip()
            detail_url_rel = row.css('td:nth-child(2) a::attr(href)').get(default='').strip()
            issued_date_raw = row.css('td:nth-child(3)::text').get(default='').strip()
            pdf_url_rel = row.css('td:nth-child(4) a::attr(href)').get(default='')
            
            # Chuẩn hóa dữ liệu
            iso_date = convert_date_to_iso8601(issued_date_raw)
            full_detail_url = response.urljoin(detail_url_rel)
            full_pdf_url = response.urljoin(pdf_url_rel)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Tạo ID duy nhất từ Tiêu đề và Ngày
            event_id = f"{title}_{iso_date}".replace('/', '-').replace('.', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT GIA TĂNG.")
                break # Thoát vòng lặp an toàn

            # 4. Đóng gói Item nếu là tin mới
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = f"Tiêu đề: {title}\nXem chi tiết: {full_detail_url}\nTải PDF: {full_pdf_url}"
            e_item['date'] = iso_date 
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

# Hàm convert chuẩn cho định dạng DD/MM/YYYY
def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    input_format = '%d/%m/%Y'
    output_format = '%Y-%m-%d'
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        return date_object.strftime(output_format)
    except ValueError:
        return None