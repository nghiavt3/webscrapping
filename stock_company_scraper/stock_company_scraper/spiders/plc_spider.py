import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_plc'
    mcpcty = 'PLC'
    allowed_domains = ['plc.petrolimex.com.vn'] 
    start_urls = ['https://plc.petrolimex.com.vn/nd/tt-co-dong.html'] 

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

        # 2. Lấy danh sách các bài đăng
        articles = response.css('.post-default')

        for article in articles:
            title = article.css('h3.post-default__title a::text').get(default='').strip()
            relative_url = article.css('h3.post-default__title a::attr(href)').get(default='')
            full_url = response.urljoin(relative_url)

            # Trích xuất Ngày đăng từ meta text nodes
            meta_parts = article.css('.post-default__meta::text').getall()
            date_str = ''
            for part in meta_parts:
                cleaned_part = part.replace('&nbsp;', '').replace('|', '').strip()
                if '/' in cleaned_part:
                    date_str = cleaned_part
                    break
            
            if not title:
                continue

            summary = title
            iso_date = convert_date_to_iso8601(date_str)

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
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None