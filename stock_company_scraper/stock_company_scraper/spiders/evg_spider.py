import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_evg'
    mcpcty = 'EVG'
    allowed_domains = ['everland.vn'] 
    start_urls = ['https://everland.vn/quan-he-co-dong/cong-bo-thong-tin'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

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

        # 2. Lấy danh sách tin bài
        records = response.css('div.article-left div.article-item')
        
        for record in records:
            title_raw = record.css('h3 a.title::text').get()
            doc_url_relative = record.css('h3 a.title::attr(href)').get()
            date_raw = record.css('h3 span.date::text').get()
            
            # Lấy tóm tắt
            description_text_nodes = record.css('p::text').getall()
            description_raw = description_text_nodes[1].strip() if len(description_text_nodes) > 1 else ""

            if not title_raw:
                continue

            cleaned_title = title_raw.strip()
            # Xử lý ngày: Everland dùng định dạng (DD.MM.YYYY)
            cleaned_date = date_raw.strip('() \n\r\t') if date_raw else None
            iso_date = convert_date_to_iso8601(cleaned_date)
            full_doc_url = response.urljoin(doc_url_relative)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{cleaned_title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{cleaned_title}\n{description_raw}\nLink: {full_doc_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None 
    try:
        # Xử lý định dạng dấu chấm đặc thù của EVG
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d.%m.%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None