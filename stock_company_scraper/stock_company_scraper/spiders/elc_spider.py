import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_elc'
    mcpcty = 'ELC' 
    allowed_domains = ['elcom.com.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def start(self):
        urls = [
            ('https://www.elcom.com.vn/co-dong/phieu-thong-tin', self.parse_generic),
            ('https://www.elcom.com.vn/co-dong/bao-cao-tai-chinh', self.parse_generic),
             
            
        ]
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                callback=callback,
                #meta={'playwright': True}
            )

    def parse_generic(self, response):
        """Hàm parse dùng chung cho các chuyên mục của SeABank"""
        # 1. Khởi tạo SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
       # cursor.execute(f'''DROP TABLE IF EXISTS {table_name}''')
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')
        # Lấy tất cả các hàng trừ hàng tiêu đề năm
        
        for section in response.css('.list-notification'):
            for block in section.css('.description-block'):

                title = block.css('.name::text').get()
                # Kết hợp ngày và tháng năm thành một chuỗi ngày đầy đủ
                date_raw = block.css('.report-content span.ml-1::text').get()
                date = " ".join(date_raw.split()) if date_raw else None
                link = block.css('.see-more a::attr(href)').get()
                if not title:
                    continue

                summary = title.strip()
                iso_date = clean_date_to_iso(date)
                absolute_url = f"{response.urljoin(link)}"

                # -------------------------------------------------------
                # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
                # -------------------------------------------------------
                event_id = f"{summary}_{iso_date if iso_date else 'NODATE'}".replace(' ', '_').strip()[:150]
                
                cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
                if cursor.fetchone():
                    self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT CHUYÊN MỤC.")
                    break 

                # 4. Yield Item
                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = summary
                e_item['date'] = iso_date
                e_item['details_raw'] = f"{summary}\nLink: {absolute_url} \n"
                e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                yield e_item

        conn.close()

def clean_date_to_iso( date_str):
        if not date_str:
            return None
        
        # 1. Làm sạch khoảng trắng thừa: "05 tháng 01, 2026"
        clean_str = " ".join(date_str.split())
        
        try:
            # 2. Thay thế chữ "tháng" và dấu phẩy để đưa về định dạng chuẩn d m Y
            # Từ: "05 tháng 01, 2026" -> "05 01 2026"
            standard_str = clean_str.replace('tháng', '').replace(',', '').strip()
            
            # 3. Parse chuỗi thành đối tượng datetime
            date_obj = datetime.strptime(standard_str, '%d %m %Y')
            
            # 4. Trả về định dạng ISO (YYYY-MM-DD)
            return date_obj.strftime('%Y-%m-%d')
        except Exception as e:
            
            return None # Trả về chuỗi gốc nếu lỗi