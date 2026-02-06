import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_ksb'
    mcpcty = 'KSB'
    allowed_domains = ['ksb.vn'] 
    start_urls = ['https://ksb.vn/quan-he-co-dong/'] 

    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
    }

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def start(self):
        yield scrapy.Request(
            url=self.start_urls[0],
            callback=self.parse,
            meta={'playwright': True},
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        
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

        rows = response.css('tbody tr') 

        for row in rows:
            # 2. Trích xuất Ngày tháng
            day = row.css('td.time div.date span:nth-child(1)::text').get()
            month_year_raw = row.css('td.time div.date span:nth-child(2)::text').get()

            if not day or not month_year_raw:
                continue

            # Ghép thành định dạng DD/MM/YYYY để parse
            date_formatted = f"{day.strip()}/{month_year_raw.strip().replace('-', '/')}"
            iso_date = convert_date_to_iso8601(date_formatted)

            # 3. Trích xuất Tiêu đề & URL
            title = row.css('td.name h4.title a::text').get().strip()
            detail_url = response.urljoin(row.css('td.detail a::attr(href)').get())
            download_url = response.urljoin(row.css('td.down a::attr(href)').get())

            # -------------------------------------------------------
            # 4. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT.")
                break 

            # 5. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"Detail: {detail_url}\nDownload: {download_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        dt_obj = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y')
        return dt_obj.strftime('%Y-%m-%d')
    except ValueError:
        return None