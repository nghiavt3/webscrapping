import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_tdc'
    mcpcty = 'TDC'
    allowed_domains = ['becamextdc.com.vn'] 
    start_urls = ['https://becamextdc.com.vn/shareholders/co-dong'] 

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

        # 2. Lặp qua từng hàng của bảng
        document_rows = response.css('.main-document table tr')

        for row in document_rows:
            # Bỏ qua hàng tiêu đề nếu có
            if row.css('th').get():
                continue

            # Trích xuất dữ liệu từ các cột (td)
            date_raw = row.css('td:nth-child(1)::text').get()
            content = row.css('td:nth-child(2)::text').get()
            download_link = row.css('td:nth-child(3) a::attr(href)').get()

            if not date_raw or not content:
                continue

            summary = content.strip()
            # Becamex TDC thường dùng DD-MM-YYYY
            iso_date = convert_date_to_iso8601(date_raw.strip())
            
            if not iso_date: continue

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
            e_item['details_raw'] = f"{summary}\nLink: {response.urljoin(download_link) if download_link else ''}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    
    # Làm sạch chuỗi: Thay thế dấu gạch chéo bằng dấu gạch ngang để đồng bộ
    clean_date = vietnam_date_str.replace('/', '-')
    
    try:
        # Thử định dạng DD-MM-YYYY
        date_object = datetime.strptime(clean_date, '%d-%m-%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None