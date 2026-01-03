import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_sgb'
    mcpcty = 'SGB'
    allowed_domains = ['saigonbank.com.vn'] 
    start_urls = ['https://www.saigonbank.com.vn/vi/quan-he-co-dong'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
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

        # 2. Duyệt qua từng khối .post
        for post in response.css('.post'):
            title = (post.css('.entry-title a::text').get() or "").strip()
            url = (post.css('.entry-title a::attr(href)').get() or "").strip()
            date_full = post.css('.post-meta span::text').get()
            
            if not title:
                continue

            # Xử lý chuỗi ngày: Loại bỏ "Ngày nhập : "
            pub_date_raw = None
            if date_full:
                pub_date_raw = date_full.replace("Ngày nhập : ", "").strip()

            summary = title
            iso_date = convert_date_to_iso8601(pub_date_raw)
            full_url = response.urljoin(url)

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
            e_item['details_raw'] = f"{summary}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        # SGB sử dụng định dạng DD/MM/YYYY HH:MM
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y %H:%M')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None