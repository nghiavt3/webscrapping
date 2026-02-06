import scrapy
import sqlite3
import os
from stock_company_scraper.items import EventItem
from datetime import datetime
import re

class EventSpider(scrapy.Spider):
    name = 'event_aps'
    mcpcty = 'APS'
    allowed_domains = ['apec.com.vn'] 
    start_urls = ['https://apec.com.vn/quan-he-co-dong/thong-tin-co-dong/','https://apec.com.vn/quan-he-co-dong/bao-cao-tai-chinh/'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        # Đường dẫn file db khớp với dự án của bạn
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # 1. Mở kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Đảm bảo bảng tồn tại (Sử dụng cấu trúc chuẩn bạn đã hiệu chỉnh)
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

        # 2. Lặp qua từng hàng trong tbody của bảng
        rows = response.css('div.analysis-content table tbody tr')
        
        for row in rows:
            # Trích xuất dữ liệu thô
            raw_date = (row.css('td.date::text').get() or "").strip()
            content = (row.css('td.content::text').get() or "").strip()
            
            # Xử lý ngày tháng và làm sạch tiêu đề
            iso_date = convert_date_to_iso8601(raw_date)
            
            # Trích xuất danh sách link tài liệu (nếu có nhiều file)
            links = row.css('td.file a::attr(href)').getall()
            formatted_links = "\n".join([response.urljoin(l) for l in links])

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Tạo ID duy nhất (Title + Date)
            event_id = f"{content}_{iso_date}".replace('/', '-').replace('.', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{content}]. DỪNG QUÉT GIA TĂNG.")
                break # Thoát vòng lặp nhẹ nhàng để Scrapy dọn dẹp các tiến trình ngầm

            # 4. Đóng gói Item nếu là tin mới
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = content
            e_item['details_raw'] = f"{content}\nFiles:\n{formatted_links}"
            e_item['date'] = iso_date 
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

# Hàm convert chuẩn của bạn
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