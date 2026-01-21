import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_hcm'
    mcpcty = 'HCM'
    allowed_domains = ['hsc.com.vn'] 
    start_urls = ['https://www.hsc.com.vn/vi/cong-bo-thong-tin','https://www.hsc.com.vn/vi/tai-chinh/bao-cao-tai-chinh'] 

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

        # 2. Chọn tất cả các khối tin tức (HSC dùng cấu trúc flex/grid với các class group)
        items = response.css('a.group.border-b')

        for item in items:
            # Trích xuất dữ liệu
            title = (item.css('h2::text').get() or "").strip()
            url = response.urljoin(item.css('::attr(href)').get())
            date_text = (item.css('p.text-neutral-700::text').get() or "").strip()
            
            if not title:
                continue

            # Chuyển đổi ngày (HSC dùng định dạng DD.MM.YYYY)
            iso_date = convert_date_to_iso8601(date_text) if date_text else "None"

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{title}\nLink: {url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    
    # HSC dùng dấu chấm: 25.12.2025
    input_format = '%d.%m.%Y'
    output_format = '%Y-%m-%d'

    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        return date_object.strftime(output_format)
    except ValueError:
        return None