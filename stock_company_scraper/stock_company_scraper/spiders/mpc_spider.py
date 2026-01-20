import scrapy
import sqlite3
import json
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_mpc'
    mcpcty = 'MPC'
    allowed_domains = ['gateway.fpts.com.vn'] 
    start_urls = ['https://gateway.fpts.com.vn/news/api/gateway/v1/mobile/list?folder=86&code=MPC&pageSize=15&selectedPage=1&cbtt=1&from=01-01-1970&to=01-01-3000&newsType=1',
                  'https://gateway.fpts.com.vn/news/api/gateway/v1/mobile/list?folder=86&code=MPC&pageSize=15&selectedPage=1&cbtt=0&from=01-01-1970&to=01-01-3000&newsType=1'] 
    
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
            self.logger.error("Lỗi parse JSON hoặc API thay đổi cấu trúc!")
            return

        for item in news_items:
            title = item.get('Title', '').strip()
            pub_date = item.get('DatePub', '')
            pdf_url = item.get('URL', '')
            
            if not title:
                continue

            iso_date = convert_date_to_iso8601(pub_date)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Sử dụng ID từ API hoặc tổ hợp Title_Date để làm khóa chính
            news_id = item.get('ID', f"{title}_{iso_date}")
            event_id = str(news_id).replace(' ', '_')[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{title}\nPDF: {pdf_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        # Xử lý chuỗi 'DD/MM/YYYY HH:MM' và loại bỏ phần mili giây nếu có
        clean_date = vietnam_date_str.split('.')[0].strip()
        date_object = datetime.strptime(clean_date, '%d/%m/%Y %H:%M')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None