import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_nha'
    mcpcty = 'NHA'
    allowed_domains = ['namhanoi.com.vn'] 
    start_urls = ['https://namhanoi.com.vn/cong-bo-thong-tin/'] 

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

        # 2. Lấy danh sách các mục PDF
        announcement_items = response.css('div.pdf-item')
        
        for item in announcement_items:
            # Trích xuất Tiêu đề và URL
            title_raw = item.css('a span::text').get()
            url_raw = item.css('a::attr(href)').get()
            
            # Trích xuất Ngày (pdf-meta)
            date_pub_raw = item.css('.pdf-meta::text').get()
            
            if not title_raw:
                continue

            summary = title_raw.strip()
            # Xử lý URL tuyệt đối
            full_url = response.urljoin(url_raw.strip()) if url_raw else ""
            # Chuyển đổi ngày
            iso_date = convert_date_to_iso8601(date_pub_raw)

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
            e_item['details_raw'] = f"{summary}\nPDF: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        # Làm sạch chuỗi ngày: loại bỏ khoảng trắng và các ký tự không phải số/gạch chéo
        clean_date = vietnam_date_str.strip()
        # Nếu có tiền tố như "Ngày: " thì loại bỏ (tùy thuộc vào giao diện web)
        clean_date = clean_date.split(':')[-1].strip()
        
        date_object = datetime.strptime(clean_date, "%d/%m/%Y")
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None