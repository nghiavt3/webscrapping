import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_tlh'
    mcpcty = 'TLH'
    allowed_domains = ['tienlensteel.com.vn'] 
    start_urls = ['https://tienlensteel.com.vn/vi/relation/0',
                  'https://tienlensteel.com.vn/vi/relation/3',
                  'https://tienlensteel.com.vn/vi/relation/2'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def start(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                # Giữ nguyên Playwright nếu trang web yêu cầu render JS
                meta={'playwright': True}
            )
        
    def parse(self, response):
        # 1. Khởi tạo SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Duyệt qua từng mục tin tức
        items = response.css('.relation-item')
        
        for item in items:
            raw_time = item.css('.relation-item__time::text').get()
            title = item.css('.relation-item__main__title::text').get()
            file_url = item.css('.relation-item__main__title::attr(href)').get()
            
            if not title or not raw_time:
                continue

            # Xử lý ngày tháng từ chuỗi "DD/MM/YYYY - HH:MM:SS"
            try:
                date_part = raw_time.split('-')[0].strip()
                iso_date = datetime.strptime(date_part, "%d/%m/%Y").strftime("%Y-%m-%d")
            except Exception:
                iso_date = datetime.now().strftime("%Y-%m-%d")

            summary = title.strip()

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
            e_item['details_raw'] = f"{summary}\nLink: {response.urljoin(file_url) if file_url else ''}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()