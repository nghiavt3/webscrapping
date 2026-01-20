import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_dig'
    mcpcty = 'DIG'
    allowed_domains = ['dic.vn'] 
    start_urls = ['https://www.dic.vn/thong-tin-co-dong'] 

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
                id TEXT PRIMARY KEY,
                mcp TEXT,
                date TEXT,
                summary TEXT,
                scraped_at TEXT,
                web_source TEXT,
                details_clean TEXT
            )
        ''')

        # 2. Chọn danh sách tin tức cổ đông
        news_items = response.css('#shareholders .item.col-md-6')
        
        for item in news_items:
            # Tiêu đề
            title = item.css('a.title::text').get()
            # Link chi tiết
            relative_url = item.css('a::attr(href)').get()
            # Ngày công bố (Lấy text bên trong thẻ span thuộc intro1)
            # Selector này tìm thẻ span có chứa thẻ i bên trong div có class intro
            date_raw = item.css('div.intro span i::text').get()
            
            
            
            if not title:
                continue

            title_clean = title.strip()
            
            iso_date = convert_date_to_iso8601(date_raw)
            absolute_url = response.urljoin(relative_url)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{title_clean}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title_clean}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title_clean
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{title_clean}\nLink: {absolute_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    input_format = "%d/%m/%Y"    
    output_format = '%Y-%m-%d'
    try:
        # Làm sạch chuỗi ngày trước khi parse (loại bỏ các dấu chấm hoặc khoảng trắng dư thừa)
        clean_str = vietnam_date_str.replace('.', '/').strip()
        date_object = datetime.strptime(clean_str, input_format)
        return date_object.strftime(output_format)
    except ValueError:
        return None