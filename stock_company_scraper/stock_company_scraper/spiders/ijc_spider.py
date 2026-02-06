import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_ijc'
    mcpcty = 'IJC'
    allowed_domains = ['becamexijc.com'] 
    start_urls = ['https://becamexijc.com/quanhecodong/'] 

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

        # 2. Lấy danh sách thông báo
        items = response.css('.item-pdf')
        
        for item in items:
            date_raw = item.css('.content time::text').get()
            title_text = item.css('.content h3 a::text').get()
            pdf_url = item.css('.content h3 a::attr(href)').get()
            
            if not title_text:
                continue

            summary = title_text.strip()
            iso_date = format_date(date_raw)
            absolute_pdf_url = response.urljoin(pdf_url)

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
            e_item['details_raw'] = f"{summary}\nLink: {absolute_pdf_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def format_date(date_string):
    """Chuyển đổi từ '3 tháng 12, 2025' sang '2025-12-03'."""
    if not date_string:
        return None
    
    # Làm sạch chuỗi
    clean_str = date_string.strip().replace('tháng', '').replace(',', '').strip()
    date_parts = clean_str.split()
    
    if len(date_parts) == 3:
        day, month, year = date_parts
        # Chuẩn hóa DD/MM/YYYY (ví dụ: 3 -> 03)
        date_combined = f"{day.zfill(2)}/{month.zfill(2)}/{year}"
        
        try:
            date_object = datetime.strptime(date_combined, "%d/%m/%Y")
            return date_object.strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None