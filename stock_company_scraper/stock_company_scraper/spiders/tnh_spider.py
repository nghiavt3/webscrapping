import scrapy
import sqlite3
import json
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_tnh'
    mcpcty = 'TNH'
    allowed_domains = ['apiweb.tnh.com.vn'] 
    start_urls = ['https://apiweb.tnh.com.vn/api-website/shareholder-relationship-news/news-by-category?locale=vn&page=1&page_size=10&id=2',
                  'https://apiweb.tnh.com.vn/api-website/shareholder-relationship-news/news-by-category?locale=vn&page=1&page_size=10&id=3',
                  'https://apiweb.tnh.com.vn/api-website/shareholder-relationship-news/news-by-category?locale=vn&page=1&page_size=10&id=4',
                  'https://apiweb.tnh.com.vn/api-website/shareholder-relationship-news/news-by-category?locale=vn&page=1&page_size=10&id=5',
                  'https://apiweb.tnh.com.vn/api-website/shareholder-relationship-news/news-by-category?locale=vn&page=1&page_size=10&id=6'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def start(self):
        """Gửi request đến API với header mô phỏng trình duyệt."""
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                headers={
                    'Accept': 'application/json',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
            )

    def parse(self, response):
        # 1. Khởi tạo SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        #cursor.execute(f'''DROP TABLE IF EXISTS {table_name}''')
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Decode JSON
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error("Không thể decode JSON!")
            return

            
        news_items = data["data"]

        # 3. Trích xuất và kiểm tra trùng lặp
        for item in news_items:
            title = item.get('title')
            pub_date = item.get('created_date')
            url = item.get('file_url')

            if not title or not pub_date:
                continue

            iso_date = convert_date_to_iso8601(pub_date)
            summary = title.strip()

            # -------------------------------------------------------
            # 4. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT.")
                break 

            # 5. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{summary}\nLink: {url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(timestamp):
    if not timestamp:
        return None
    try:
        iso_date_only = datetime.fromtimestamp(timestamp / 1000).date().isoformat()
        return iso_date_only
    except ValueError:
        return None