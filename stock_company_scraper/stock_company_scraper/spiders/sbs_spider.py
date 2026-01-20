import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_sbs'
    mcpcty = 'SBS'
    allowed_domains = ['sbsc.com.vn'] 
    start_urls = ['https://sbsc.com.vn/Annoucement.aspx'] 

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

        # 2. Lấy danh sách tin tức từ bảng tin của SBS
        # Selector sử dụng chứa chuỗi (contains) để tránh ID ASP.NET thay đổi
        news_items = response.css('div[id*="cbSearchHeadNews"] table table tr')

        for item in news_items:
            title = item.css('a.newsTitle::text').get()
            detail_url = item.css('a.newsTitle::attr(href)').get()
            raw_date = item.css('span.timestamp::text').get()

            if not title:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso8601(raw_date) if raw_date else "1970-01-01"
            full_url = response.urljoin(detail_url) if detail_url else ""

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
        # Làm sạch chuỗi ngày tháng trước khi parse
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None