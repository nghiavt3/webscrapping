import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_htv'
    mcpcty = 'HTV' 
    allowed_domains = ['vitrichem.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def start(self):
        urls = [
            ('https://vitrichem.vn/quan-he-co-dong/cong-bo-thong-tin/', self.parse_generic),
            ('https://vitrichem.vn/quan-he-co-dong/bao-cao-tai-chinh/', self.parse_generic),
             ('https://vitrichem.vn/dai-hoi-co-dong-thuong-nien-nam-2025/', self.parse_generic),
             
            
        ]
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                callback=callback,
                #meta={'playwright': True}
            )

    async def parse_generic(self, response):
        """Hàm parse dùng chung cho các chuyên mục của SeABank"""
        # 1. Khởi tạo SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        #cursor.execute(f'''DROP TABLE IF EXISTS {table_name}''')
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')
        for item in response.css('.prettyListItems a.prettylink'):
            
            
            
            title = item.css('.fileTitle::text').get()
            # Trích xuất danh sách các thẻ span float_right
            # span[0] thường là Size, span[1] là Date
            spans = item.css('span.float_right::text').getall()
            
            # Xử lý làm sạch dữ liệu
            file_size = spans[0].replace('|', '').strip() if len(spans) > 0 else None
            date_post = spans[1].strip() if len(spans) > 1 else None

            link = item.css('::attr(href)').get()
            if not title:
                continue

            summary = title.strip()
            iso_date = convert_to_iso(date_post)
            absolute_url = response.urljoin(link)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{iso_date if iso_date else 'NODATE'}".replace(' ', '_').strip()[:150]
            
            # cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            # if cursor.fetchone():
            #     self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT CHUYÊN MỤC.")
            #     break 

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

def convert_to_iso(date_str):
    # Xóa các ký tự st, nd, rd, th sau con số
    clean_str = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', date_str)
    # Convert "7 Jan 2026" -> datetime object
    try:
        dt = datetime.strptime(clean_str, '%d %b %Y')
        return dt.strftime('%Y-%m-%d')
    except:
        return None