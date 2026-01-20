import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_vig'
    mcpcty = 'VIG'
    allowed_domains = ['visc.com.vn'] 
    start_urls = ['https://visc.com.vn/vi/news/cong-bo-thong-tin-3103.spp'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # 1. Khởi tạo kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Chọn danh sách bài viết
        articles = response.css('#ctl00_pnlArticleList > a')

        for art in articles:
            # Lấy ngày từ thuộc tính data-date
            raw_date = art.css('::attr(data-date)').get()
            title = (art.css('h2::text').get() or "").strip()
            link = (art.css('::attr(href)').get() or "").strip()
            
            if not title or not raw_date:
                continue

            summary = title
            iso_date = convert_date_to_iso8601(raw_date)

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
            
            full_url = response.urljoin(link)
            e_item['details_raw'] = f"{summary}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    # Xử lý trường hợp chuỗi có cả giờ: "31/12/2025 08:30"
    clean_date = vietnam_date_str.split(' ')[0].strip()
    try:
        date_object = datetime.strptime(clean_date, '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None