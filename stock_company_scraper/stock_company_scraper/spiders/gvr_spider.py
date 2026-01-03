import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_gvr'
    mcpcty = 'GVR'
    allowed_domains = ['rubbergroup.vn'] 
    start_urls = ['https://rubbergroup.vn/quan-he-co-dong/cong-bo-thong-tin'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
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

        # 2. Lọc các hàng chứa file tài liệu (tr[class^="file"])
        rows = response.css('table.tbl > tbody > tr[class^="file"]')
        
        for row in rows:
            # Trích xuất tiêu đề và ngày (nằm chung trong td thứ 2)
            title = row.css('td:nth-child(2) section p::text').get()
            date_raw = row.css('td:nth-child(2) span.date2::text').get()
            download_url = row.css('td:nth-child(3) a::attr(href)').get()

            if not title:
                continue

            # Làm sạch dữ liệu
            cleaned_title = title.strip()
            # Loại bỏ dấu ngoặc đơn quanh ngày: (25/12/2025) -> 25/12/2025
            clean_date_str = date_raw.strip('()').strip() if date_raw else ""
            iso_date = convert_date_to_iso8601(clean_date_str)
            full_url = response.urljoin(download_url)

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
            e_item['details_raw'] = f"{cleaned_title}\nLink: {full_url}"
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