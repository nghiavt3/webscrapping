import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_x26'
    mcpcty = 'X26'
    allowed_domains = ['ezsearch.fpts.com.vn'] 
    start_urls = ['https://ezsearch.fpts.com.vn/Services/EzData/default2.aspx?s=1963'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        yield scrapy.Request(
            url=self.start_urls[0],
            callback=self.parse,
            meta={'playwright': True}
        )

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

        # 2. Lấy các container chứa tin tức
        news_containers = response.css('#Table8 td > table')
        
        for container in news_containers:
            title = container.css('tr:first-child a::text').get()
            url = container.css('tr:first-child a::attr(href)').get()
            datetime_str = container.css('tr:last-child span::text').get()

            if not title or not datetime_str:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso8601(datetime_str.strip())

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
    # FPTS format thường là 'DD/MM/YYYY HH:MM'
    input_format = '%d/%m/%Y %H:%M'
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        # Dự phòng nếu chỉ có ngày 'DD/MM/YYYY'
        try:
            return datetime.strptime(vietnam_date_str.strip()[:10], '%d/%m/%Y').strftime('%Y-%m-%d')
        except:
            return None