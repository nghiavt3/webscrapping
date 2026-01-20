import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_tlg'
    mcpcty = 'TLG'
    allowed_domains = ['thienlonggroup.com'] 
    start_urls = ['https://thienlonggroup.com/quan-he-co-dong/tat-ca'] 

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

        # 2. Duyệt qua danh sách tin tức
        items = response.css('.list-news-2 .item')
        
        for item in items:
            title = item.css('div.title a::text').get()
            raw_date = item.css('span.date::text').get()
            download_url = item.css('a.down::attr(href)').get()

            if not title or not raw_date:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso8601(raw_date.strip())
            
            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Tạo ID duy nhất dựa trên nội dung và ngày
            event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT.")
                break 

            # 4. Đóng gói Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{summary}\nLink: {response.urljoin(download_url) if download_url else 'N/A'}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        # Thiên Long dùng định dạng DD.MM.YYYY
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d.%m.%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None