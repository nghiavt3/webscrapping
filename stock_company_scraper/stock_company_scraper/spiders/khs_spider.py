import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_khs'
    mcpcty = 'KHS' 
    allowed_domains = ['kihuseavn.com'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        urls = [
            ('http://kihuseavn.com/tt-1/quan-he-co-dong/', self.parse_generic),
            
             
            
        ]
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                callback=callback,
                #meta={'playwright': True}
            )

    def parse_generic(self, response):
        """Hàm parse dùng chung cho các chuyên mục của SeABank"""
        # 1. Khởi tạo SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        #cursor.execute(f'''DROP TABLE IF EXISTS {table_name}''')
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')
        
        for item in response.css('.tin-tuc'):
            # Lấy chuỗi thời gian thô: "Thời gian đăng : 30-12-2025 | Lượt xem: 43"
            raw_time_text = item.css('.thoi-gian-tin::text').get()
            
            publish_date = None
            views = 0
            
            if raw_time_text:
                # Dùng Regex trích xuất ngày (định dạng dd-mm-yyyy)
                date_match = re.search(r'(\d{2}-\d{2}-\d{4})', raw_time_text)
                if date_match:
                    publish_date = date_match.group(1)
                
                # Dùng Regex trích xuất số lượt xem
                view_match = re.search(r'Lượt xem:\s*(\d+)', raw_time_text)
                if view_match:
                    views = view_match.group(1)

            title = item.css('.tieu-de-tin a::text').get()
            date = publish_date
            link = item.css('.tieu-de-tin a::attr(href)').get()
            if not title:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso8601(date)
            absolute_url = f"{response.urljoin(link)}"

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{iso_date if iso_date else 'NODATE'}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT CHUYÊN MỤC.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{summary}\nLink: {absolute_url} \n"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d-%m-%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None