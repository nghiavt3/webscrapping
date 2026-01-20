import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_vea'
    mcpcty = 'VEA'
    allowed_domains = ['veamcorp.com'] 
    start_urls = ['http://veamcorp.com/quan-he-co-dong/cong-bo-thong-tin-114.html'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # 1. Khởi tạo SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Lấy danh sách thẻ tin tức
        featured_news_cards = response.css('div.col-md-9 .card.box-catnew')

        for article in featured_news_cards:
            title_tag = article.css('.card-body .title-new')
            title_raw = title_tag.css('::text').get()
            url_raw = title_tag.css('::attr(href)').get()
            
            if not title_raw or not url_raw:
                continue
                
            summary = title_raw.strip()
            
            # Trích xuất và làm sạch Ngày đăng
            date_raw = article.css('.card-body .text-date-new::text').get()
            # Loại bỏ tiền tố "Ngày đăng:"
            date_pub = date_raw.replace('Ngày đăng:', '').strip() if date_raw else None
            iso_date = convert_date_to_iso8601(date_pub)

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
            
            full_url = response.urljoin(url_raw)
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