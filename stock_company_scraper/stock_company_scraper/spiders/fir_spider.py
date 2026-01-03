import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_fir'
    mcpcty = 'FIR'
    allowed_domains = ['fir.vn'] 
    start_urls = ['https://fir.vn/vn/quan-he-co-dong/cong-bo-thong-tin/'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={"playwright": True}
            )

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

        # 2. Duyệt qua từng item trong danh sách report-list
        items = response.css('div.report-list div.item')

        for item in items:
            # Trích xuất ngày tháng từ cấu trúc card
            raw_day = item.css('.number-card h5::text').get()          # "06"
            raw_month_year = item.css('.number-card p::text').get()    # "12-2025"
            
            title = item.css('.content a::text').get()
            file_url = item.css('.content a::attr(href)').get()

            if not title or not raw_day or not raw_month_year:
                continue

            # Xử lý ngày tháng sang ISO
            full_date_str = f"{raw_day.strip()}-{raw_month_year.strip()}"
            iso_date = None
            try:
                date_obj = datetime.strptime(full_date_str, '%d-%m-%Y')
                iso_date = date_obj.strftime('%Y-%m-%d')
            except Exception:
                continue

            cleaned_title = title.strip()
            full_doc_url = response.urljoin(file_url)

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
            e_item['details_raw'] = f"{cleaned_title}\nLink: {full_doc_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()