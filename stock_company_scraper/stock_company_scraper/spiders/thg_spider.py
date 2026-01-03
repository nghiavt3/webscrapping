import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_thg'
    mcpcty = 'THG'
    allowed_domains = ['ticco.com.vn'] 
    start_urls = ['https://ticco.com.vn/quan-he-co-dong/cong-bo-thong-tin/'] 

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

        # 2. Lấy danh sách các item bài viết
        items = response.css('div.list.grid > div.item')

        for item in items:
            # Trích xuất Ngày công bố (TICCO dùng định dạng DD.MM.YYYY)
            published_date = item.css('time::text').get()
            # Trích xuất Tiêu đề
            title = item.css('div.title span.line-clamp-3::text').get()
            # Lấy data-src để tìm popup tương ứng (ví dụ: #popup-document-3672)
            data_src = item.css('a.download-file::attr(data-src)').get()

            if not title or not published_date:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso8601(published_date.strip())
            
            # Truy tìm link file trong popup ẩn (nằm ngoài item hiện tại nhưng trong response)
            file_url = None
            if data_src:
                # Tìm thẻ a::attr(href) bên trong div có ID trùng với data_src
                file_url = response.css(f'{data_src} div.content a::attr(href)').get()

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
            
            full_file_link = response.urljoin(file_url) if file_url else "N/A"
            e_item['details_raw'] = f"{summary}\nLink file: {full_file_link}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        # TICCO sử dụng dấu chấm (.) làm phân cách: 31.12.2025
        date_object = datetime.strptime(vietnam_date_str, '%d.%m.%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None