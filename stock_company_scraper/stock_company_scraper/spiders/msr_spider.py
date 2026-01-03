import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_msr'
    mcpcty = 'MSR'
    allowed_domains = ['masanhightechmaterials.com'] 
    start_urls = ['https://masanhightechmaterials.com/vi/investor_category/thong-bao-cong-ty/'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # 1. Thiết lập kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        release_items = response.css('div.releases-item')
        
        for item in release_items:
            # Lấy ngày công bố (phần văn bản cuối cùng sau icon)
            date_raw = item.css('div.date::text').getall()
            date_text = date_raw[-1].strip() if date_raw else None
            
            # Trích xuất tiêu đề và URL
            title_node = item.css('h4 a')
            title = title_node.css('::text').get()
            url = title_node.css('::attr(href)').get()
            
            if not title:
                continue
                
            summary = title.strip().replace('\xa0', ' ')
            iso_date = convert_date_to_iso8601(date_text)

            # -------------------------------------------------------
            # 2. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Tạo ID duy nhất từ tiêu đề và ngày
            event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT.")
                break 

            # 3. Yield dữ liệu
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{summary}\nLink: {response.urljoin(url)}"
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