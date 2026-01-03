import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_ors'
    mcpcty = 'ORS'
    allowed_domains = ['tpbs.com.vn'] 
    # Quét cả 2 chuyên mục: Công bố thông tin và Thông tin cổ đông
    start_urls = [
        'https://tpbs.com.vn/vi/thong-tin-tps/quan-he-co-dong/cong-bo-thong-tin',
        'https://tpbs.com.vn/vi/thong-tin-tps/quan-he-co-dong/thong-tin-co-dong'
    ] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # 1. Kết nối SQLite và tạo bảng nếu chưa có
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Lặp qua từng hàng của bảng
        for row in response.css('tr.itemRow'):
            title_node = row.css('td a::text').get()
            link_node = row.css('td a::attr(href)').get()
            date_iso = row.css('td i::text').get() # TPS trả về dạng YYYY-MM-DD

            if not title_node:
                continue

            summary = title_node.strip()
            iso_date = date_iso.strip() if date_iso else "1970-01-01"
            full_url = response.urljoin(link_node)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. BỎ QUA HÀNG NÀY.")
                continue # Dùng continue thay vì break vì trang này có thể trộn lẫn tin theo 2 URL

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