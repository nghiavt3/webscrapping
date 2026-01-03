import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_kdh'
    mcpcty = 'KDH'
    allowed_domains = ['khangdien.com.vn'] 
    start_urls = ['https://www.khangdien.com.vn/co-dong/cong-bo-thong-tin'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # 1. Kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Lấy danh sách thông báo
        announcements = response.css('div.stockcol')

        for ann in announcements:
            link_selector = ann.css('a')
            url_raw = link_selector.css('::attr(href)').get()
            date_raw = link_selector.css('i::text').get()
            
            # Làm sạch ngày tháng (xóa ngoặc đơn)
            clean_date_str = date_raw.strip('() \n') if date_raw else None
            iso_date = convert_date_to_iso8601(clean_date_str)

            # Trích xuất Tiêu đề sạch
            all_text_nodes = link_selector.css('::text').getall()
            title_parts = [t.strip() for t in all_text_nodes if t.strip() and t.strip() != date_raw.strip()]
            title = title_parts[0] if title_parts else ""

            if not title:
                continue

            full_url = response.urljoin(url_raw)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{title}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    input_format = "%d/%m/%Y"    
    output_format = '%Y-%m-%d'
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        return date_object.strftime(output_format)
    except ValueError:
        return None