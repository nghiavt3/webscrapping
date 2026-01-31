import scrapy
import sqlite3
import re
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_vgc'
    mcpcty = 'VGC'
    allowed_domains = ['viglacera.com.vn'] 
    start_urls = ['https://viglacera.com.vn/document-category/cong-bo-thong-tin',
                  'https://viglacera.com.vn/document-category/bao-cao-thuong-nien'] 

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

        # 2. Lấy danh sách văn bản
        list_items = response.css('ul.qhcd_list li')

        for item in list_items:
            title = item.css('a::text').get(default='').strip()
            url_raw = item.css('a::attr(href)').get(default='').strip()

            if not title:
                continue

            # Tự động trích xuất ngày tháng từ tiêu đề nếu có (VD: "Thông báo... ngày 20/05/2024")
            date_match = re.search(r'(\d{2}/\d{2}/\d{4})', title)
            if date_match:
                iso_date = convert_date_to_iso8601(date_match.group(1))
            else:
                # Nếu không có ngày trong tiêu đề, dùng ngày hiện tại làm mốc tham chiếu
                iso_date = 'NODATE'

            summary = title

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
            if iso_date == 'NODATE':
                e_item['date'] = None
            else :
                e_item['date'] = iso_date
            
            full_url = response.urljoin(url_raw)
            e_item['details_raw'] = f"{summary}\nLink: {full_url}"
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