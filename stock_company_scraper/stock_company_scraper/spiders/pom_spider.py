import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_pom'
    mcpcty = 'POM'
    allowed_domains = ['pomina-steel.com'] 
    start_urls = ['http://www.pomina-steel.com/co-dong.html'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def parse(self, response):
        # 1. Khởi tạo kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Lấy danh sách các khối tin tức
        for item in response.css('div.list-box'):
            title = item.css('div.r-text p::text').get()
            link = response.urljoin(item.css('a::attr(href)').get())
            raw_date = item.css('span.r-date::text').get()
            
            if not title:
                continue

            summary = title.strip()
            
            # Xử lý ngày tháng đặc thù (DD-MM-YYYY)
            iso_date = "1970-01-01"
            if raw_date:
                try:
                    date_obj = datetime.strptime(raw_date.strip(), '%d-%m-%Y')
                    iso_date = date_obj.strftime('%Y-%m-%d')
                except ValueError:
                    iso_date = raw_date # Giữ nguyên nếu không parse được

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