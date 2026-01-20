import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
from urllib.parse import urlparse, parse_qs
class EventSpider(scrapy.Spider):
    name = 'event_pvi'
    mcpcty = 'PVI'
    allowed_domains = ['pviholdings.com.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        urls = [
            ('https://pviholdings.com.vn/vi/announcement', self.parse_generic),
            ('https://pviholdings.com.vn/vi/meeting', self.parse_generic),
           
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
            title = item.css('.text h5::text').get()
            raw_link = item.css('a.text::attr(href)').get(default='')
            date = item.css('.time::text').get()
            
            # Xử lý link: Nếu là link download file PDF, ta trích xuất tham số filePath

            final_link = raw_link
            if 'downloadFile' in raw_link:
                parsed_url = urlparse(raw_link)
                file_path = parse_qs(parsed_url.query).get('filePath', [None])[0]
                if file_path:
                    # Chuyển //pvi.com.vn/... thành https://pvi.com.vn/...
                    final_link = "https:" + file_path if file_path.startswith('//') else file_path
            else:
                # Nếu là link tương đối (announcement-detail...), nối với domain
                final_link = response.urljoin(raw_link)

            if not title:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso8601(date)
            absolute_url = final_link

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
            e_item['details_raw'] = f"{summary}\n\nLink: {absolute_url} \n"
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