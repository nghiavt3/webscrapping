import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_hdc'
    mcpcty = 'HDC'
    allowed_domains = ['hodeco.vn'] 
    start_urls = ['https://hodeco.vn/shareholder/1'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

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

        # 2. Selector chính: Chọn tất cả các khối tài liệu riêng lẻ
        documents = response.css('div.wrap-cd div.item-cd')

        for doc in documents:
            # Trích xuất Tiêu đề và URL
            title_selector = doc.css('span.name-cd a')
            title_raw = title_selector.css('::text').get()
            url_raw = title_selector.css('::attr(href)').get()
            
            # Trích xuất Ngày/Giờ và làm sạch (bỏ dấu ngoặc đơn)
             # Ngày giờ nằm trong span.date-cd
            date_time_raw = doc.css('span.date-cd::text').get()
            # Xử lý làm sạch: loại bỏ ký tự ngoặc đơn và khoảng trắng dư thừa
            date_time = date_time_raw.strip('() \n') if date_time_raw else None

            if not title_raw:
                continue

            title = title_raw.strip()
            iso_date = (date_time)
            full_url = response.urljoin(url_raw)

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

