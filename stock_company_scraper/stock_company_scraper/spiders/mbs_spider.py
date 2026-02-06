import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_mbs'
    mcpcty = 'MBS'
    allowed_domains = ['mbs.com.vn'] 
    start_urls = ['https://www.mbs.com.vn/tin-co-dong/',
                  'https://www.mbs.com.vn/bao-cao-tai-chinh/',
                  'https://www.mbs.com.vn/cong-bo-thong-tin/'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def start(self):
        for  url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={'playwright': True}
            )
        
    async def parse(self, response):
        # 1. Kết nối SQLite và khởi tạo bảng nếu chưa có
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Lặp qua từng card tin tức
        cards = response.css('div.group')

        for card in cards:
            title = (card.css('h3 a::text').get() or "").strip()
            detail_url = card.css('h3 a::attr(href)').get()
            raw_date = card.css('p.text-\\[\\#363939\\]::text').get()
            
            if not title:
                continue

            iso_date = convert_date_to_iso8601(raw_date)
            full_url = response.urljoin(detail_url)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{title}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    input_format = '%d/%m/%Y'
    output_format = '%Y-%m-%d'
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        return date_object.strftime(output_format)
    except ValueError:
        return None