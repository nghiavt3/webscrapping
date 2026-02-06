import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_yeg'
    mcpcty = 'YEG'
    allowed_domains = ['yeah1group.com'] 
    start_urls = ['https://yeah1group.com/investor-relation/announcements',
                  'https://yeah1group.com/investor-relation/financial-statements',
                  'https://yeah1group.com/investor-relation/shareholders-meeting'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # 1. Khởi tạo SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Duyệt qua các thông báo (mỗi li là một item)
        announcements = response.css('ul.w-full > li')
        
        for item in announcements:
            title_raw = item.css('h4::text').get()
            datetime_raw = item.css('span.order-last.text-sm::text').get()
            link = item.css('span.download__icon a.ir__link::attr(href)').get()

            if not title_raw or not datetime_raw:
                continue

            summary = title_raw.strip()
            iso_date = convert_date_to_iso8601(datetime_raw.strip())

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
            
            full_url = response.urljoin(link) if link else "N/A"
            e_item['details_raw'] = f"{summary}\nLink PDF: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    # Định dạng của Yeah1: 'HH:MM DD/MM/YYYY'
    input_format = '%H:%M %d/%m/%Y'
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None