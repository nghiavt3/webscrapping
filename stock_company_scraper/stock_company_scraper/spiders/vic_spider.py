import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
from scrapy_playwright.page import PageMethod

class EventSpider(scrapy.Spider):
    name = 'event_vic'
    mcpcty = 'VIC'
    allowed_domains = ['vingroup.net'] 
    start_urls = ['https://vingroup.net/quan-he-co-dong/cong-bo-thong-tin'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        yield scrapy.Request(
            url=self.start_urls[0],
            callback=self.parse,
            meta={
                'playwright': True,
                'playwright_launch_kwargs': {
                    'headless': False,  # Chế độ headful để qua mặt bot detection
                },
                'playwright_page_methods': [
                    PageMethod('wait_for_selector', 'div.news-list-container') 
                ]
            }
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

        # 2. Chọn tất cả các mục tài liệu
        records = response.css('div.ir-document-item')
        
        for record in records:
            title_raw = record.css('a span::text').get()
            url_raw = record.css('a::attr(href)').get()
            date_raw = record.css('em::text').get()
            
            if not title_raw or not date_raw:
                continue

            summary = title_raw.strip()
            iso_date = convert_date_to_iso8601(date_raw.strip())

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
            e_item['date'] = iso_date
            e_item['summary'] = summary
            
            full_url = response.urljoin(url_raw) if url_raw else "N/A"
            e_item['details_raw'] = f"{summary}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None