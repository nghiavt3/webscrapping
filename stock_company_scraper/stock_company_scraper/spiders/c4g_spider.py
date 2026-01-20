import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_c4g'
    mcpcty = 'C4G'
    allowed_domains = ['cienco4.vn'] 
    start_urls = ['https://cienco4.vn/quanhe_codong_cat/quan-he-co-dong/'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # 1. Kết nối SQLite và chuẩn bị bảng
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY,
                mcp TEXT,
                date TEXT,
                summary TEXT,
                scraped_at TEXT,
                web_source TEXT,
                details_clean TEXT
            )
        ''')

        # 2. Duyệt qua danh sách tin tức
        news_items = response.css('div.list-new-4-col > div.col')
        
        for item in news_items:
            anchor = item.css('a')
            url = anchor.css('::attr(href)').get()
            title = anchor.css('h3::text').get()
            date_full = anchor.css('div.thoi-gian-qhcd p::text').get()

            # Làm sạch dữ liệu
            clean_title = title.strip() if title else ""
            clean_date_raw = date_full.replace('Cập nhật ngày', '').strip() if date_full else ""
            iso_date = convert_date_to_iso8601(clean_date_raw)
            full_url = response.urljoin(url)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{clean_title}_{iso_date}".replace('/', '-').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{clean_title}]. DỪNG QUÉT GIA TĂNG.")
                break 

            # 4. Đóng gói Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = clean_title
            e_item['details_raw'] = f"{clean_title}\nLink: {full_url}"
            e_item['date'] = iso_date 
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    input_format = '%d/%m/%Y'
    output_format = '%Y-%m-%d'
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        return date_object.strftime(output_format)
    except ValueError:
        return None