import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_ocb'
    mcpcty = 'OCB'
    allowed_domains = ['ocb.com.vn'] 
    start_urls = ['https://ocb.com.vn/vi/nha-dau-tu'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

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

        # 2. Duyệt qua từng nhóm danh mục tin tức
        groups = response.css('div.content__item')

        for group in groups:
            # Lặp qua từng bản tin trong danh mục đó
            news_items = group.css('.content__info-item')
            
            for item in news_items:
                title = item.css('a::text').get()
                link = item.css('a::attr(href)').get()
                raw_date = item.css('.published-date::text').get()
                
                if not title:
                    continue

                summary = title.strip()
                # Làm sạch ngày (loại bỏ "Ngày đăng: ")
                clean_date_str = raw_date.replace('Ngày đăng:', '').strip() if raw_date else None
                iso_date = convert_date_to_iso8601(clean_date_str)
                full_url = response.urljoin(link)

                # -------------------------------------------------------
                # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
                # -------------------------------------------------------
                event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
                
                cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
                if cursor.fetchone():
                    self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT.")
                    # Vì các tin được sắp xếp theo thời gian, gặp tin cũ ta có thể dừng group này
                    continue 

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
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None