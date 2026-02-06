import scrapy
import sqlite3
import json
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_sav'
    mcpcty = 'SAV'
    allowed_domains = ['gateway.fpts.com.vn'] 
    start_urls = ['https://gateway.fpts.com.vn/news/api/gateway/v1/mobile/list?folder=86&code=SAV&pageSize=8&selectedPage=1&cbtt=1&from=01-01-1970&to=01-01-3000&newsType=1',
                  'https://gateway.fpts.com.vn/news/api/gateway/v1/mobile/list?folder=86&code=SAV&pageSize=8&selectedPage=1&cbtt=0&from=01-01-1970&to=01-01-3000&newsType=1'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def start(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                headers={
                    'Accept': 'application/json',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
            )

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

        # 2. Decode JSON
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            return

        if data.get("Code") != 0 or "Table1" not in data.get("Data", {}):
            return
            
        news_items = data["Data"]["Table1"]

        for item in news_items:
            title = item.get('Title')
            pub_date_raw = item.get('DatePub') # Ví dụ: "25/12/2025 10:30"
            news_url = item.get('URL').replace('\\', '/').replace(' ', '%20')

            if not title:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso8601(pub_date_raw)

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
            e_item['details_raw'] = f"{summary}\nLink: {news_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        # Tách lấy phần ngày giờ trước dấu chấm (nếu có miliseconds)
        clean_date = vietnam_date_str.split('.')[0].strip()
        # API FPTS trả về định dạng DD/MM/YYYY HH:MM
        date_object = datetime.strptime(clean_date, '%d/%m/%Y %H:%M')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None