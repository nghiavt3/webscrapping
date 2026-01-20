import scrapy
import sqlite3
import os
from stock_company_scraper.items import EventItem
from datetime import datetime
import re

class EventSpider(scrapy.Spider):
    name = 'event_aah'
    allowed_domains = ['thanhopnhat.com'] 
    start_urls = ['https://thanhopnhat.com/category/quan-he-co-dong/'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        # Kết nối SQLite để kiểm tra tin cũ
        # Chú ý: Đường dẫn file db phải khớp với nơi bạn lưu trữ
        self.db_path = 'stock_events.db'
        
    def parse(self, response):
        post_items = response.css('div.col.post-item')
        
        # Mở kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Đảm bảo bảng tồn tại để không bị lỗi khi SELECT
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
        
        for item in post_items:
            # 1. Trích xuất Tiêu đề và URL
            title_selector = item.css('h5.post-title a')
            title = title_selector.css('::text').get().strip() if title_selector else None
            url = title_selector.css('::attr(href)').get() if title_selector else None
            
            # 2. Trích xuất Ngày đăng
            date_raw = item.css('div.post-meta::text').get().strip() if item.css('div.post-meta::text') else None
            iso_date = convert_date_to_iso8601(date_raw)

            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # Tạo ID theo quy tắc giống hệt trong SQLiteStoragePipeline
            event_id = f"{title}_{iso_date}".replace('/', '-').replace('.', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT GIA TĂNG.")
                conn.close()
                break # DỪNG TOÀN BỘ SPIDER NGAY TẠI ĐÂY

            # 4. Trích xuất Tóm tắt
            excerpt = item.css('p.from_the_blog_excerpt::text').get()
            if excerpt:
                excerpt = excerpt.strip().replace('\xa0', '').replace('\n', '')

            # 5. Đưa dữ liệu vào Item nếu là tin mới
            e_item = EventItem()
            e_item['mcp'] = 'AAH'
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n'+ str(excerpt) +'\n' + str(url)
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
    except ValueError:
        return None