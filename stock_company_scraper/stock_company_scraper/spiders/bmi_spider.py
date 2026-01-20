import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_bmi'
    mcpcty = 'BMI'
    allowed_domains = ['baominh.com.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        urls = [
            ('https://www.baominh.com.vn/cong-bo-thong-tin-bmi', self.parse_generic),
            ('https://www.baominh.com.vn/dai-hoi-dong-co-dong', self.parse_generic),
             ('https://www.baominh.com.vn/bao-cao-tai-chinh', self.parse_generic),
             
             
            
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

        for item in response.css('div.item'):
            title = item.css('h2.header a::text').get()
            date_str = item.css('.time .block:nth-child(1) span::text').get()
            # 3. Trích xuất danh sách các file đính kèm
            files = []
            file_elements = item.css('.flies-download ul li')
            
            for f in file_elements:
                file_name = f.css('a::text').get()
                file_href = f.css('a::attr(href)').get()
                
                if file_name and file_href:
                    files.append({
                        'file_name': file_name.strip(),
                        'file_url': response.urljoin(file_href.strip())
                    })

            if not title or not date_str:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso8601(date_str)
            absolute_url = files

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
            
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

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d-%m-%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None