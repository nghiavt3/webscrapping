import scrapy
import sqlite3
import re
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_no1'
    mcpcty = 'NO1'
    allowed_domains = ['911group.com.vn'] 
    start_urls = ['https://911group.com.vn/cong-bo-thong-tin'] 

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

        # 2. Lấy danh sách các item tin tức
        items = response.css('div.col-12.mb-4') 
        date_regex = r'(\d{2}/\d{2}/\d{4})'
        
        for item in items:
            title = item.css('h3 a::text').get()
            if not title:
                continue
                
            summary = title.strip()
            detail_url = response.urljoin(item.css('h3 a::attr(href)').get())
            
            # Trích xuất nội dung mô tả để tìm ngày
            excerpt = item.css('div.news-content > p:not(.news-time)::text').get()
            excerpt_cleaned = excerpt.strip() if excerpt else ""
            
            # Tách ngày tháng bằng Regex
            published_date = None
            if excerpt_cleaned:
                match = re.search(date_regex, excerpt_cleaned)
                if match:
                    published_date = match.group(1)
            
            iso_date = convert_date_to_iso8601(published_date)

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
            e_item['details_raw'] = f"{summary}\n{excerpt_cleaned}\nLink: {detail_url}"
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