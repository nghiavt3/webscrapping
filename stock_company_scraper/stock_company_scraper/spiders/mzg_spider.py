import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_mzg'
    mcpcty = 'MZG'
    allowed_domains = ['miza.vn'] 
    start_urls = ['https://miza.vn/vi/quan-he-co-dong/cong-bo-thong-tin'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def parse(self, response):
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

        # 2. Lấy danh sách các box thông báo
        list_items = response.css('div.border.border-gray-200.rounded-lg.p-4')

        for item in list_items:
            title = item.css('h4.text-lg.font-semibold::text').get()
            summary_desc = item.css('p.text-gray-600.text-sm::text').get()
            date_raw = item.css('div.flex.items-center.gap-4 span::text').get()
            
            if not title:
                continue

            title_cleaned = title.strip()
            iso_date = convert_date_to_iso8601(date_raw)
            details = summary_desc.strip() if summary_desc else "Không có tóm tắt."

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{title_cleaned}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title_cleaned}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title_cleaned
            e_item['details_raw'] = f"Title: {title_cleaned}\nDesc: {details}"
            e_item['date'] = iso_date
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