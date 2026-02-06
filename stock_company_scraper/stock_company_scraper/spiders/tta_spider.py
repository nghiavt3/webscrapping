import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_tta'
    mcpcty = 'TTA'
    allowed_domains = ['truongthanhgroup.com.vn'] 
    start_urls = ['https://truongthanhgroup.com.vn/co-dong/cong-bo-thong-tin/','https://truongthanhgroup.com.vn/co-dong/bao-cao-tai-chinh/'] 

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

        # 2. Duyệt qua các dòng trong bảng
        rows = response.css('tr.item-shareholder')
        
        for row in rows:
            # Ngày Đăng: cột thứ hai
            pub_date = row.css('td:nth-child(2) span::text').get()
            # Nội dung: cột thứ ba
            title_raw = row.css('td:nth-child(3) div.title-shareholder::text').get()
            # Link Tải về: cột thứ tư
            file_url = row.css('td:nth-child(4) a::attr(href)').get()
            
            if not pub_date or not title_raw:
                continue

            summary = title_raw.strip()
            iso_date = convert_date_to_iso8601(pub_date.strip())

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item
            item = EventItem()
            item['mcp'] = self.mcpcty
            item['web_source'] = self.allowed_domains[0]
            item['summary'] = summary
            item['date'] = iso_date
            item['details_raw'] = f"{summary}\nLink: {response.urljoin(file_url) if file_url else 'N/A'}"
            item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None