import scrapy
import sqlite3
import re
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_nbb'
    mcpcty = 'NBB'
    allowed_domains = ['nbb.com.vn'] 
    start_urls = ['http://nbb.com.vn/vi-vn/zone/557/item/1871/item.cco'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

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

        # 2. Chọn khối "Các tin khác" và duyệt qua các thẻ h5
        news_items = response.css('div.otheritem h5')

        for item in news_items:
            title_raw = item.css('a::text').get()
            if not title_raw:
                continue
                
            summary = title_raw.strip()
            relative_url = item.css('a::attr(href)').get()
            absolute_url = response.urljoin(relative_url)

            # 3. Kỹ thuật tách ngày từ chuỗi tiêu đề (Regex)
            # Ví dụ tiêu đề: "Thông báo ngày 25.12.2025 về việc..."
            date_match = re.search(r'(\d{1,2}\.\d{1,2}\.\d{4})', summary)
            iso_date = None
            if date_match:
                date_str = date_match.group(1)
                iso_date = convert_date_to_iso8601(date_str)
            else:
                # Nếu không tìm thấy ngày trong tiêu đề, dùng ngày quét làm mặc định
                iso_date = datetime.now().strftime('%Y-%m-%d')

            # -------------------------------------------------------
            # 4. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_NODATE".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT.")
                break 

            # 5. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['date'] = None
            e_item['details_raw'] = f"{summary}\nLink: {absolute_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        # NBB thường dùng dấu chấm (.) thay vì gạch chéo (/)
        clean_date = vietnam_date_str.replace('/', '.').strip()
        date_object = datetime.strptime(clean_date, '%d.%m.%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None