import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_bab'
    mcpcty = 'BAB'
    allowed_domains = ['baca-bank.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'
    
    async def start(self):
        year = datetime.now().year
        urls = [
            (f"https://www.baca-bank.vn/SitePages/website/quan-he-co-dong.aspx?t=C%C3%B4ng%20b%E1%BB%91%20th%C3%B4ng%20tin&y={year}&s=QHCD&ac=QUAN%20H%E1%BB%86%20C%E1%BB%94%20%C4%90%C3%94NG", self.parse_generic),
           # (f"https://www.baca-bank.vn/SitePages/website/quan-he-co-dong.aspx?ac=QUAN%20H%E1%BB%86%20C%E1%BB%94%20%C4%90%C3%94NG&t=C%C3%B4ng%20b%E1%BB%91%20th%C3%B4ng%20tin&y=2026&skh=&ty=&nbh=&s=QHCD&Page=2", self.parse_generic),

            (f"https://www.baca-bank.vn/SitePages/website/quan-he-co-dong.aspx?t=B%C3%A1o%20c%C3%A1o%20t%C3%A0i%20ch%C3%ADnh&y={year}&s=QHCD&ac=QUAN%20H%E1%BB%86%20C%E1%BB%94%20%C4%90%C3%94NG", self.parse_generic),
                            
            #(f"https://www.baca-bank.vn/SitePages/website/quan-he-co-dong.aspx?ac=QUAN%20H%E1%BB%86%20C%E1%BB%94%20%C4%90%C3%94NG&t=B%C3%A1o%20c%C3%A1o%20t%C3%A0i%20ch%C3%ADnh&y=2025&skh=&ty=&nbh=&s=QHCD&Page=2", self.parse_generic),
        ]
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                callback=callback,
                #meta={'playwright': True}
            )

    def parse_generic(self, response):
        """Hàm parse dùng chung cho các chuyên mục"""
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

        # Lấy tất cả các khối bao quanh bản tin
        # Lấy danh sách các item
        # Chọn tất cả các hàng trong thân bảng
        rows = response.css('table.table tbody tr')

        for row in rows:
            date = row.css('td:nth-child(3)::text').get()
            title = row.css('td:nth-child(5)::text').get()
            file_url = row.css('td:nth-child(6) a::attr(href)').get()
            absolute_url = response.urljoin(file_url)
            summary = title
            iso_date = convert_date_to_iso8601(date)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # if iso_date :
            #     event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
            # else :
            #     event_id = f"{summary}_NODATE".replace(' ', '_').strip()[:150]
            
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
            e_item['details_raw'] = f"{summary}\nLink: {absolute_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item
        conn.close()

        next_page = response.css('div.paging li.active + li a::attr(href)').get()
        if next_page and 'javascript' not in next_page:
            yield response.follow(next_page, callback=self.parse_generic)
        

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None