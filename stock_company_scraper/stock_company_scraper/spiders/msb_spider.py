import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
from scrapy_playwright.page import PageMethod

class EventSpider(scrapy.Spider):
    name = 'event_msb'
    mcpcty = 'MSB'
    allowed_domains = ['msb.com.vn'] 
    start_urls = ['https://www.msb.com.vn/vi/nha-dau-tu/cong-bo-thong-tin.html'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                meta={
                    "playwright": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_load_state", "networkidle"),
                        PageMethod("wait_for_timeout", 2000), 
                    ],
                },
                callback=self.parse
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

        # 2. Bóc tách danh sách các baocao-item
        bao_cao_items = response.css('.baocao-item')

        for item in bao_cao_items:
            # Lấy ngày công bố bằng cách loại bỏ tiền tố văn bản
            date_raw = item.css('p::text').get()
            ngay_cong_bo = date_raw.replace('Ngày công bố:', '').strip() if date_raw else None
            
            title_raw = item.css('h3::text').get()
            if not title_raw:
                continue
            
            summary = title_raw.strip()
            iso_date = convert_date_to_iso8601(ngay_cong_bo)
            
            url_relative = item.css('div.d-flex a::attr(href)').get()
            url_absolute = response.urljoin(url_relative) if url_relative else ""

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
            e_item['details_raw'] = f"{summary}\nPDF: {url_absolute}"
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