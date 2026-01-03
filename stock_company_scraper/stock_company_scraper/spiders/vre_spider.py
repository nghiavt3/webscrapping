import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_vre'
    mcpcty = 'VRE'
    # Website IR của Vincom Retail
    allowed_domains = ['ir.vincom.com.vn'] 
    start_urls = ['https://ir.vincom.com.vn/cong-bo-thong-tin/cong-bo-thong-tin-vi/'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # 1. Thiết lập kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Duyệt qua danh sách tin tức (layout cột)
        for post in response.css('div.post-list-resource div.column'):
            item_node = post.css('div.item')
            
            # Trích xuất Tiêu đề: Ưu tiên lấy từ attribute title của thẻ a
            title = item_node.css('h6 a::attr(title)').get()
            if not title:
                title = item_node.css('h6 a::text').get()

            # Trích xuất URL tài liệu
            url = item_node.css('h6 a::attr(href)').get()

            # Trích xuất Ngày: Lấy định dạng máy (ISO) từ attribute datetime
            raw_datetime = item_node.css('time::attr(datetime)').get()
            iso_date = raw_datetime.split('T')[0] if raw_datetime else None

            if not title or not iso_date:
                continue

            summary = title.strip()

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
            
            full_url = response.urljoin(url) if url else "N/A"
            e_item['details_raw'] = f"{summary}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()