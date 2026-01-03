import scrapy
import sqlite3
import re
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_frt'
    mcpcty = 'FRT'
    allowed_domains = ['frt.vn'] 
    start_urls = ['https://frt.vn/quan-he-co-dong'] 

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

        # 2. Duyệt qua các khối tài liệu báo cáo
        # Sử dụng ^= để chọn các class bắt đầu bằng "reports_file"
        for item in response.css('div[class^="reports_file"]'):
            title = item.css('div[class^="reports_txt"]::text').get()
            pdf_url = item.css('a[class^="reports_title"]::attr(href)').get()
            
            if not title:
                continue

            # 3. Trích xuất ngày tháng từ URL (VD: .../20251119_...)
            iso_date = None
            if pdf_url:
                date_match = re.search(r'(\d{4})(\d{2})(\d{2})', pdf_url)
                if date_match:
                    year, month, day = date_match.groups()
                    iso_date = f"{year}-{month}-{day}"

            cleaned_title = title.strip()
            full_pdf_url = response.urljoin(pdf_url)

            # -------------------------------------------------------
            # 4. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Nếu không lấy được ngày từ URL, dùng ngày hiện tại hoặc để trống tùy nhu cầu
            display_date = iso_date if iso_date else datetime.now().strftime('%Y-%m-%d')
            event_id = f"{cleaned_title}_{display_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                break 

            # 5. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{cleaned_title}\nLink PDF: {full_pdf_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()