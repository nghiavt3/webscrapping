import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_vds'
    mcpcty = 'VDS'
    allowed_domains = ['vdsc.com.vn'] 
    start_urls = ['https://vdsc.com.vn/quan-he-co-dong/cong-bo-thong-tin'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def parse(self, response):
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

        # 2. Lặp qua các mục tin tức (dạng cột)
        items = response.css('div.news-4 > div.col-md-3')
        
        for item in items:
            date_full = item.css('a.item .text-content span::text').get()
            title = item.css('a.item h6.title::text').get()
            detail_url = item.css('a.item::attr(href)').get()
            
            if not title or not date_full:
                continue

            summary = title.strip()
            iso_date = convert_date_dash_to_iso8601(date_full.strip())

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
            
            full_url = response.urljoin(detail_url) if detail_url else "N/A"
            e_item['details_raw'] = f"{summary}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_dash_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        # Xử lý định dạng DD-MM-YYYY
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d-%m-%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None