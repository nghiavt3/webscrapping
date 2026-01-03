import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_crc'
    mcpcty = 'CRC'
    allowed_domains = ['createcapital.vn'] 
    start_urls = ['https://createcapital.vn/quan-he-co-dong-crc.htm'] 

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
                id TEXT PRIMARY KEY,
                mcp TEXT,
                date TEXT,
                summary TEXT,
                scraped_at TEXT,
                web_source TEXT,
                details_clean TEXT
            )
        ''')

        # 2. Duyệt qua các item tin tức
        for item in response.css('div.item'):
            link_selector = item.css('a')
            url = link_selector.css('::attr(href)').get()
            title_raw = link_selector.css('h3.dot3::text').get()
            summary_raw = link_selector.css('p::text').get()

            if not title_raw:
                continue

            # Làm sạch dữ liệu
            cleaned_title = title_raw.strip()
            cleaned_summary = summary_raw.replace('[...]', '').strip() if summary_raw else ""
            full_url = response.urljoin(url)
            scraped_date = datetime.now().strftime('%Y-%m-%d')

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Vì trang này không có ngày đăng ở danh mục, ta dùng Title làm ID
            event_id = f"{cleaned_title}_NODATE".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_title
            e_item['date'] = None #scraped_date  # Gán tạm ngày hiện tại vì web không có ngày
            e_item['details_raw'] = f"Title: {cleaned_title}\nExcerpt: {cleaned_summary}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()