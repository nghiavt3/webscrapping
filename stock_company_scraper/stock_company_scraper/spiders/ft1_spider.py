import scrapy
import json
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_ft1'
    mcpcty = 'FT1'
    allowed_domains = ['gateway.fpts.com.vn'] 
    # API URL với các tham số folder=86 (CBTT) và code=FT1
    start_urls = ['https://gateway.fpts.com.vn/news/api/gateway/v1/mobile/list?folder=86&code=FT1&pageSize=8&selectedPage=1&cbtt=0&from=01-01-1970&to=01-01-3000&newsType=1',
                  'https://gateway.fpts.com.vn/news/api/gateway/v1/mobile/list?folder=86&code=FT1&pageSize=8&selectedPage=1&cbtt=1&from=01-01-1970&to=01-01-3000&newsType=1'] 

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
        # 1. Kết nối SQLite và chuẩn bị bảng
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Giải mã JSON
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error("Không thể decode JSON!")
            return

        # 3. Truy cập mảng dữ liệu Table1
        news_items = data.get("Data", {}).get("Table1", [])
        if not news_items:
            self.logger.warning("API không trả về tin tức nào.")
            return

        for item in news_items:
            title = item.get('Title')
            pub_date = item.get('DatePub') # Định dạng thường là 'DD/MM/YYYY HH:mm'
            doc_url = item.get('URL')

            if not title:
                continue

            cleaned_title = title.strip()
            iso_date = convert_date_to_iso8601(pub_date)
            
            # -------------------------------------------------------
            # 4. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{cleaned_title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                break 

            # 5. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{cleaned_title}\nLink: {doc_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    # Format API: '30/12/2025 15:30'
    input_format = '%d/%m/%Y %H:%M'
    output_format = '%Y-%m-%d'
    try:
        # Cắt bỏ phần milliseconds nếu có (split tại dấu chấm)
        clean_str = vietnam_date_str.split('.')[0].strip()
        date_object = datetime.strptime(clean_str, input_format)
        return date_object.strftime(output_format)
    except ValueError:
        return None