import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_dhg'
    mcpcty = 'DHG' 
    allowed_domains = ['dhgpharma.com.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        urls = [
            ('https://dhgpharma.com.vn/vi/thong-bao-co-dong', self.parse_generic),
            ('https://dhgpharma.com.vn/vi/dai-hoi-co-dong', self.parse_generic),
            ('https://dhgpharma.com.vn/vi/bao-cao-tai-chinh', self.parse_generic),
            
             
            
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
        #cursor.execute(f'''DROP TABLE IF EXISTS {table_name}''')
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')
        # Lấy tất cả các hàng trừ hàng tiêu đề năm
        
        for row in response.css('div.views-row'):         
            title = row.css('div.field-field-display-title h3 a::text').get(default='').strip()
            date = "".join(row.css('div.views-row-date-published ::text').getall()).strip()
            link = row.css('div.field-field-display-title h3 a::attr(href)').get()
            pdf_link = row.css('div.file-pdf a[href$=".pdf"]::attr(href)').get()
            if not title:
                continue

            summary = title.strip()
            iso_date = manual_iso_date(date)
            absolute_url = f"{response.urljoin(link)}\n{response.urljoin(pdf_link)}"

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

def manual_iso_date(date_str):
    # Giả sử date_str = "5 Tháng 1, 2026"
    months = {
        'Tháng 1': '01', 'Tháng 2': '02', 'Tháng 3': '03', 'Tháng 4': '04',
        'Tháng 5': '05', 'Tháng 6': '06', 'Tháng 7': '07', 'Tháng 8': '08',
        'Tháng 9': '09', 'Tháng 10': '10', 'Tháng 11': '11', 'Tháng 12': '12'
    }
    date_str = date_str.strip().replace(',', '')
    parts = date_str.split(' ') # ['5', 'Tháng', '1', '2026']
    
    day = parts[0].zfill(2)
    month = parts[2].zfill(2)
    year = parts[3]
    
    return f"{year}-{month}-{day}"

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None