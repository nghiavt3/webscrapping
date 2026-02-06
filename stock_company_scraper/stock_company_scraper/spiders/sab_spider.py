import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_sab'
    mcpcty = 'SAB'
    allowed_domains = ['sabeco.com.vn'] 
    current_year = datetime.now().year
    last_year = current_year -1
    start_urls = [
        f'https://www.sabeco.com.vn/co-dong/cong-bo-thong-tin/{current_year}',
        f'https://www.sabeco.com.vn/co-dong/bao-cao-tai-chinh/{current_year}-2'
        f'https://www.sabeco.com.vn/co-dong/cong-bo-thong-tin/{last_year}',
        f'https://www.sabeco.com.vn/co-dong/bao-cao-tai-chinh/{last_year}-2'
        ] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # 1. Kết nối và khởi tạo SQLite
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

        # 2. Duyệt qua từng bài viết
        items = response.css('div.financy-report ul li')
        for item in items:
            title = item.css('a::text').get()
            full_text = "".join(item.css('::text').getall())
            date_match = re.search(r'\((\d{1,2}/\d{1,2}/\d{4})\)', full_text)
            date = date_match.group(1) if date_match else None
            link = item.css('a::attr(href)').get()
            
            if not title:
                continue

            summary = title.strip()
            # Làm sạch chuỗi ngày tháng (Sabeco thường để định dạng DD/MM/YYYY)
            
            iso_date = convert_date_to_iso8601(date)
            full_url = response.urljoin(link).replace(" ", "%20")

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