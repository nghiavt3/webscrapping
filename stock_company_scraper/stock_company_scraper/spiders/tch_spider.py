import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_tch'
    mcpcty = 'TCH'
    allowed_domains = ['hoanghuy.vn'] 
    start_urls = ['https://www.hoanghuy.vn/quan-he-co-dong/'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def clean_and_format_date(self, date_pub_raw):
        """Hàm giúp làm sạch chuỗi ngày và chuyển đổi định dạng."""
        if not date_pub_raw:
            return None
            
        # Loại bỏ phần chữ "Cập nhật:" và khoảng trắng thừa
        date_str = date_pub_raw.replace('Cập nhật:', '').strip()
        
        try:
            # %d/%m/%Y xử lý tốt cả 7/11/2025 và 28/11/2025
            datetime_object = datetime.strptime(date_str, '%d/%m/%Y')
            return datetime_object.strftime('%Y-%m-%d')
        except ValueError:
            return None

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

        # 2. Duyệt qua danh sách tin tức
        for item in response.css('ul.codong li'):
            title = item.css('h3 a::text').get()
            relative_url = item.css('h3 a::attr(href)').get()
            date_raw = item.css('p::text').get()
            
            # Làm sạch ngày tháng
            iso_date = self.clean_and_format_date(date_raw)
            
            if not title or not iso_date:
                continue

            summary = title.strip()
            full_url = response.urljoin(relative_url)

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
            e_item['details_raw'] = f"{summary}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()