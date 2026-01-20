import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_shb'
    mcpcty = 'SHB'
    allowed_domains = ['shb.com.vn'] 
    start_urls = ['https://www.shb.com.vn/category/nha-dau-tu/cong-bo-thong-tin/'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
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

        # 2. Duyệt qua danh sách tin tức (.item_ndt)
        for item in response.css('.item_ndt'):
            # Trích xuất link
            link = item.css('.title a::attr(href)').get()
            
            # Trích xuất ngày tháng và loại bỏ dấu ngoặc đơn ()
            raw_date = item.css('.title a span.time::text').get()
            clean_date_str = raw_date.strip('() ') if raw_date else None
            
            # Trích xuất tiêu đề bằng XPath để lấy text trực tiếp từ <a> (bỏ qua <span>)
            title = item.xpath('.//div[@class="title"]/a/text()').get()
            
            if not title:
                continue

            summary = title.strip()
            # SHB dùng định dạng DD-MM-YYYY
            iso_date = convert_date_to_iso8601(clean_date_str)
            full_url = response.urljoin(link)

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
        # SHB thường dùng dấu gạch ngang '-' thay vì gạch chéo '/'
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d-%m-%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None