import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_mvn'
    mcpcty = 'MVN' 
    allowed_domains = ['vimc.co'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        urls = [
            ('https://vimc.co/chuyen-muc/cong-bo-thong-tin/cong-bo-thong-tin-hoat-dong/', self.parse_generic),
            ('https://vimc.co/chuyen-muc/cong-bo-thong-tin/thong-bao/', self.parse_generic),
             ('https://vimc.co/chuyen-muc/quan-he-co-dong/bao-cao-hoat-dong/', self.parse_generic),
             ('https://vimc.co/chuyen-muc/quan-he-co-dong/tin-tuc-cho-nha-dau-tu/', self.parse_generic),
             
            
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
        articles = response.css('article')
        
        for art in articles:     
            title = art.css('h3.entry-title a::text').get()
            date = art.css('div.vina-date::text').get()
            link = art.css('h3.entry-title a::attr(href)').get()
            excerpt = art.css('div.entry-content p::text').get()
            if not title:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso8601(date)
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
            e_item['details_raw'] = f"{summary}\n {excerpt} \n Link: {absolute_url} \n"
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