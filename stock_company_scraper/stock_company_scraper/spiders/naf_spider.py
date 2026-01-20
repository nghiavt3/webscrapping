import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_naf'
    mcpcty = 'NAF'
    allowed_domains = ['nafoods.com'] 
    start_urls = ['https://www.nafoods.com/shareholders-relations/announcement'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # 1. Kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Nhắm vào bảng TablePress
        rows = response.css('table#tablepress-4 tbody tr')
        
        for row in rows:
            title_raw = row.css('td.column-1::text').get()
            date_raw = row.css('td.column-2::text').get()
            link = row.css('td.column-3 a::attr(href)').get()

            if not title_raw:
                continue

            summary = title_raw.strip()
            
            # Xử lý ngày tháng đặc thù DD/MM/YY
            iso_date = None
            if date_raw:
                try:
                    clean_date = date_raw.strip()
                    # %y xử lý năm 2 chữ số (ví dụ: 24 -> 2024)
                    date_obj = datetime.strptime(clean_date, '%d/%m/%y')
                    iso_date = date_obj.strftime('%Y-%m-%d')
                except ValueError:
                    iso_date = None

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{summary}\nLink: {link}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()