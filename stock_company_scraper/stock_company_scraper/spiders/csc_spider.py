import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_csc'
    mcpcty = 'CSC'
    allowed_domains = ['cotanagroup.vn'] 
    start_urls = ['https://www.cotanagroup.vn/thong-bao-cua-hdqt/'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
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

        # 2. Duyệt qua từng hàng trong bảng
        rows = response.css('table.table > tbody > tr')

        for row in rows:
            ten_tai_lieu = row.css('td > div.doc-name::text').get()
            link_tai_ve = row.css('td > a.bnt-dl::attr(href)').get()
            ten_file_tai_ve = row.css('td > a.bnt-dl::attr(download)').get()

            if not ten_tai_lieu:
                continue

            # Làm sạch dữ liệu
            cleaned_title = ten_tai_lieu.strip()
            full_url = response.urljoin(link_tai_ve)
            scraped_date = datetime.now().strftime('%Y-%m-%d')

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Tạo ID duy nhất từ tên tài liệu
            event_id = f"{cleaned_title}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_title
            e_item['date'] = scraped_date
            e_item['details_raw'] = f"Tài liệu: {cleaned_title}\nLink: {full_url}\nFile: {ten_file_tai_ve}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()