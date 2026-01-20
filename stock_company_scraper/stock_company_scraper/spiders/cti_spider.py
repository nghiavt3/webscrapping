import scrapy
import json
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_cti'
    mcpcty = 'CTI'
    allowed_domains = ['gateway.fpts.com.vn'] 
    
    # API URL lấy danh sách công bố thông tin
    start_urls = [
        'https://gateway.fpts.com.vn/news/api/gateway/v1/mobile/list?folder=86&code=CTI&pageSize=8&selectedPage=1&cbtt=0&from=01-01-1970&to=01-01-3000&newsType=1',
        'https://gateway.fpts.com.vn/news/api/gateway/v1/mobile/list?folder=86&code=CTI&pageSize=8&selectedPage=1&cbtt=1&from=01-01-1970&to=01-01-3000&newsType=1'
        ] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
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

        # 2. Parse JSON
        try:
            data = json.loads(response.text)
            news_items = data.get("Data", {}).get("Table1", [])
        except (json.JSONDecodeError, KeyError):
            self.logger.error("Lỗi cấu trúc API hoặc JSON!")
            return

        # 3. Duyệt tin tức
        for item in news_items:
            title = item.get("Title", "").strip()
            date_pub = item.get("DatePub", "") # Định dạng: "DD/MM/YYYY HH:MM"
            raw_url = item.get("URL", "")
            final_url = raw_url.replace('\\', '/')

            iso_date = convert_date_to_iso8601(date_pub)

            # -------------------------------------------------------
            # 4. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL)
            # -------------------------------------------------------
            event_id = f"{title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT.")
                break 

            # 5. Đóng gói Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{title}\n{final_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    # Lưu ý: Định dạng từ API là '%d/%m/%Y %H:%M'
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y %H:%M')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None