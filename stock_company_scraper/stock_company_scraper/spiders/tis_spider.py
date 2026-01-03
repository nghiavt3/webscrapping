import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_tis'
    mcpcty = 'TIS'
    allowed_domains = ['tisco.com.vn'] 
    start_urls = ['https://tisco.com.vn/quan-he-co-dong/thong-bao.html'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

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

        # 2. Xử lý tin nổi bật (First News)
        first_news = response.css('.first-news')
        if first_news:
            title = first_news.css('.title a::text').get()
            date = first_news.css('.date::text').get()
            url = response.urljoin(first_news.css('.title a::attr(href)').get())
            
            if title and date:
                # Làm sạch date: "Cập nhật 20/05/2025" -> "20/05/2025"
                clean_date = date.replace('Cập nhật ', '').strip()
                yield from self.process_item(cursor, table_name, title, clean_date, url)

        # 3. Xử lý danh sách tin bên dưới
        items = response.css('.items-bottom li')
        for item in items:
            raw_date = item.css('.date::text').get()
            title = item.css('.title a::text').get()
            url = response.urljoin(item.css('.title a::attr(href)').get())
            
            if title and raw_date:
                clean_date = raw_date.replace('Cập nhật ', '').strip()
                # Nếu gặp tin cũ trong danh sách này, logic process_item sẽ trả về stop_signal
                stop_signal = list(self.process_item(cursor, table_name, title, clean_date, url))
                if not stop_signal:
                    break
                for i in stop_signal: yield i

        conn.close()

    def process_item(self, cursor, table_name, title, clean_date, url):
        """Hỗ trợ kiểm tra trùng lặp và đóng gói Item"""
        summary = title.strip()
        iso_date = convert_date_to_iso8601(clean_date)
        
        # -------------------------------------------------------
        # 4. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
        # -------------------------------------------------------
        event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
        
        cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
        if cursor.fetchone():
            self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT.")
            return # Thoát hàm nếu trùng
            
        e_item = EventItem()
        e_item['mcp'] = self.mcpcty
        e_item['web_source'] = self.allowed_domains[0]
        e_item['summary'] = summary
        e_item['date'] = iso_date
        e_item['details_raw'] = f"{summary}\nLink: {url}"
        e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        yield e_item

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None