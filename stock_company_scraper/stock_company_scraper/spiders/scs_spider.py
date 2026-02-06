import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_scs'
    mcpcty = 'SCS'
    allowed_domains = ['scsc.vn'] 
    start_urls = ['https://www.scsc.vn/vn/info_category.aspx?IDCAT=34','https://www.scsc.vn/vn/info_category.aspx?IDCAT=36'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def parse(self, response):
        # 1. Khởi tạo kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Selector chính từ DevExpress layout
        announcement_items = response.css('td.dxncItem')
        flag_date = ''
        
        for item_selector in announcement_items:
            title_raw = item_selector.css('div.dxncItemHeader a::text').get()
            url_raw = item_selector.css('div.dxncItemHeader a::attr(href)').get()
            date_raw = item_selector.css('div.dxncItemDate::text').get()
            
            if not title_raw:
                continue

            # Xử lý ngày: Dùng flag_date nếu dòng hiện tại bị trống ngày
            if date_raw:
                pub_date = date_raw.strip()
                flag_date = pub_date
            else:
                pub_date = flag_date

            summary = title_raw.strip()
            iso_date = convert_date_to_iso8601(pub_date)
            full_url = response.urljoin(url_raw)

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