import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_pac'
    mcpcty = 'PAC'
    allowed_domains = ['pinaco.com'] 
    start_urls = ['https://www.pinaco.com/co-dong/thong-tin-khac-68.html',
                  'https://www.pinaco.com/co-dong/bao-cao-tai-chinh-66.html',
                  'https://www.pinaco.com/co-dong/dai-hoi-co-dong-64.html'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def parse(self, response):
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

        # 2. Lấy danh sách thông báo
        items = response.css('.colum-list li')

        for item in items:
            title = item.css('p a::text').get()
            url = item.css('p a::attr(href)').get()
            
            # Trích xuất Ngày từ text node
            text_nodes = item.css('p::text').getall()
            # Theo cấu trúc web Pinaco, ngày thường ở node thứ 3 (index 2) hoặc node có chứa dấu '|'
            raw_date = None
            for node in text_nodes:
                if '|' in node:
                    raw_date = node.split('|')[0].strip()
                    break
            
            if not title:
                continue

            summary = title.strip()
            # Pinaco dùng định dạng YYYY-MM-DD sẵn, nhưng ta vẫn bọc lại để đảm bảo tính an toàn
            iso_date = raw_date if raw_date else "1970-01-01"
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