import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
from scrapy_playwright.page import PageMethod
import json
import re
class EventSpider(scrapy.Spider):
    name = 'event_sls'
    mcpcty = 'SLS' 
    allowed_domains = ['miaduongsonla.vn','gateway.fpts.com.vn'] 
    start_urls = [
        'https://gateway.fpts.com.vn/news/api/gateway/v1/mobile/list?folder=86&code=SLS&pageSize=8&selectedPage=1&cbtt=0&from=01-01-1970&to=01-01-3000&newsType=1',
        'https://gateway.fpts.com.vn/news/api/gateway/v1/mobile/list?folder=86&code=SLS&pageSize=8&selectedPage=1&cbtt=1&from=01-01-1970&to=01-01-3000&newsType=1'
                  ] 
    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        urls = [
            ('https://miaduongsonla.vn/thong-tin-co-dong1', self.parse_generic), 
            ('https://miaduongsonla.vn/bao-cao-tai-chinh', self.parse_generic), 
            
        ]
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                callback=callback,
                meta={
                    "playwright": True,
                    "playwright_page_methods": [
                    PageMethod("wait_for_load_state", "networkidle"),
                    PageMethod("screenshot", path="error.png", full_page=True),
                    ],
                },
            )

        for url in self.start_urls:    
            yield scrapy.Request(
                    url=url,
                    callback=self.parse_json,
                    headers={
                        'Accept': 'application/json',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                    }
                )    

    def parse_generic(self, response):
        """Hàm parse dùng chung cho các chuyên mục của SeABank"""
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
        # Lấy tất cả các hàng trừ hàng tiêu đề năm
        articles = response.css('article.article-item')
        
        for article in articles: 
            title = article.css('.article-title a::text').get(default='').strip()
            date = article.css('.post-date::text').get()
            link = article.css('.article-title a::attr(href)').get()
            if not title:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso8601(date)
            absolute_url = f"{response.urljoin(link)}"

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{iso_date if iso_date else 'NODATE'}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT CHUYÊN MỤC.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{summary}\nLink: {absolute_url} \n"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

    def parse_json(self, response):
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

        # 2. Giải mã JSON
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error("Lỗi giải mã JSON!")
            return

        if data.get("Code") != 0 or "Table1" not in data.get("Data", {}):
            return
            
        news_items = data["Data"]["Table1"]

        # 3. Trích xuất và kiểm tra trùng lặp
        for item in news_items:
            title = item.get('Title')
            pub_date = item.get('DatePub')
            url = item.get('URL')

            if not title or not pub_date:
                continue

            iso_date = convert_date_to_iso8601_ver2(pub_date)
            summary = title.strip()

            # --- KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC) ---
            event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT.")
                break 

            # 4. Gán Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{summary}\nLink: {url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601_ver2(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        # Xử lý chuỗi có thể chứa miligiây sau dấu chấm
        clean_date = vietnam_date_str.split('.')[0].strip()
        date_object = datetime.strptime(clean_date, '%d/%m/%Y %H:%M')
        return date_object.strftime('%Y-%m-%d')
    except (ValueError, IndexError):
        return None
def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), '%H:%M:%S / %d-%m-%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None