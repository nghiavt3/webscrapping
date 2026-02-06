import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_las'
    mcpcty = 'LAS'
    allowed_domains = ['supelamthao.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def start(self):
        urls = [
            ('https://supelamthao.vn/chuyen-muc-co-dong/49-thong-tin-co-dong', self.parse_generic),
            ('https://supelamthao.vn/bao-cao/46-bao-cao-tai-chinh', self.parse_bctc),
             
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
        for item in response.css('div.item'):
            title = item.css('.title::text').get()
            # 2. Trích xuất và làm sạch ngày tháng
            # Vì HTML lồng nhiều lớp date-display-single, ta lấy text từ các span con
            day = item.css('.news-data > .date-display-single span::text').get()
            month_year = item.css('.font .date-display-single span::text').getall()
            
            # Ghép lại thành chuỗi ngày tháng hoàn chỉnh: "20/10/2025"
            full_date = ""
            if day and len(month_year) >= 2:
                # month_year[0] là "T 10", ta lấy số 10
                month = month_year[0].replace('T', '').strip()
                year = month_year[1].strip()
                full_date = f"{day.strip()}/{month}/{year}"
            # 3. Lấy link bài viết
            link = item.css('.more a::attr(href)').get()    
            if not title:
                continue

            summary = title.strip()
            iso_date = (full_date)
            absolute_url = response.urljoin(link)

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

    async def parse_bctc(self, response):
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

        for tab_content in response.css('div.cls-tab-shareholder-tab-content'):
            # Lấy ID (ví dụ: tab2025C) và cắt chữ 'tab' với 'C' để lấy năm
            raw_id = tab_content.css('::attr(id)').get()
            year = raw_id.replace('tab', '').replace('C', '') if raw_id else "Unknown"
            
            # Lặp qua từng mục download trong năm đó
            for item in tab_content.css('div.item-download'):
                title = item.css('div.content a::text').get()
                link = item.css('a.link-download::attr(href)').get()
                date = item.css('div.date-block span::text').get()
                if not title:
                    continue

                summary = title.strip()
                iso_date = convert_date_to_iso8601(date)
                absolute_url = response.urljoin(link)

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

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), '%Y-%m-%d %H:%M:%S')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None