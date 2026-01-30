import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_lcg'
    mcpcty = 'LCG'
    allowed_domains = ['lizen.vn'] 
    start_urls = ['https://lizen.vn/vi/document-category/thong-bao-co-dong?page=1',
                  'https://lizen.vn/vi/document-category/bao-cao-tai-chinh',
                  'https://lizen.vn/vi/document-category/dai-hoi-dong-co-dong'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def parse(self, response):
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

        # 2. Chọn tất cả các hàng dữ liệu
        records = response.css('table.data-recruitment tbody tr')
        
        for record in records:
            title_raw = record.css('td.col-item1 a::text').get()
            if not title_raw:
                continue
                
            summary = title_raw.strip()
            article_url = record.css('td.col-item1 a::attr(href)').get()
            date_raw = record.css('td:nth-child(2)::text').get()
            download_url = record.css('td:nth-child(5) a::attr(href)').get()

            iso_date = convert_date_to_iso8601(date_raw)
            abs_article = response.urljoin(article_url)
            abs_download = response.urljoin(download_url)

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
            e_item['details_raw'] = f"Title: {summary}\nArticle: {abs_article}\nDownload: {abs_download}"
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