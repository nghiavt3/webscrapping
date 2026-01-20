import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_ntc'
    mcpcty = 'NTC'
    allowed_domains = ['namtanuyen.com.vn'] 
    start_urls = ['https://namtanuyen.com.vn/danh-muc/thong-bao-co-dong'] 

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

        # 2. Chọn các khối tin tức dựa trên style border-bottom
        news_blocks = response.css('div.container > div.row[style*="border-bottom"]')
        
        for block in news_blocks:
            link_tag = block.css('div.col-8 a')
            tieu_de = link_tag.css('::text').get()
            relative_url = link_tag.css('::attr(href)').get()
            datetime_str = block.css('div.col-4::text').get()
            
            if not tieu_de:
                continue

            summary = tieu_de.strip()
            iso_date = convert_date_to_iso8601(datetime_str)
            full_url = response.urljoin(relative_url) if relative_url else ""

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

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        # Định dạng của NTC: 'DD/MM/YYYY HH:MM:SS'
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y %H:%M:%S')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        # Fallback nếu format ngày bị thay đổi (không có giờ)
        try:
            date_object = datetime.strptime(vietnam_date_str.strip()[:10], '%d/%m/%Y')
            return date_object.strftime('%Y-%m-%d')
        except:
            return None