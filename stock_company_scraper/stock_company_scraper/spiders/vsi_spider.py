import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_vsi'
    mcpcty = 'VSI'
    allowed_domains = ['waseco.com.vn'] 
    start_urls = ['http://waseco.com.vn/quan-he-co-dong/thong-bao/',
                  'http://waseco.com.vn/quan-he-co-dong/bao-cao-tai-chinh/',
                  'http://waseco.com.vn/quan-he-co-dong/nghi-quyet/'] 

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

        # 2. Duyệt qua từng mục báo cáo (.baocao-item)
        for item in response.css('.baocao-item'):
            # Cột 2 chứa ngày đăng
            pub_date_raw = item.css('.vc_col-sm-2 .wpb_wrapper p::text').get()
            
            # Cột 10 chứa tiêu đề và link
            title = item.css('.vc_col-sm-10 .wpb_wrapper p a::text').get()
            url = item.css('.vc_col-sm-10 .wpb_wrapper p a::attr(href)').get()

            if not title or not pub_date_raw:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso8601(pub_date_raw.strip())

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
            
            full_url = response.urljoin(url) if url else "N/A"
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