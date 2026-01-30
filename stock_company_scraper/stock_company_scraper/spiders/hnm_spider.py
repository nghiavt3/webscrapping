import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_hnm'
    mcpcty = 'HNM'
    allowed_domains = ['hanoimilk.com'] 
    start_urls = ['http://www.hanoimilk.com/blogs/dai-hoi-co-dong',
                  'http://www.hanoimilk.com/blogs/bao-cao-tai-chinh'] 

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
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Lấy danh sách tất cả các thông báo
        article_items = response.css('.article-item')

        for item in article_items:
            title_raw = item.css('.article-title a::text').get()
            url = item.css('.article-title a::attr(href)').get()
            date_nodes = item.css('.article-date::text').getall()

            # Lọc nhiễu từ các thẻ SVG/Text rỗng
            date_raw_list = [text.strip() for text in date_nodes if text.strip()]
            date_str = date_raw_list[-1] if date_raw_list else None
            
            if not title_raw:
                continue

            title = title_raw.strip()
            iso_date = convert_date_to_sqlite_format(date_str)
            absolute_url = response.urljoin(url)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Tạo ID duy nhất từ tiêu đề và ngày
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
            e_item['details_raw'] = f"{title}\nLink: {absolute_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_sqlite_format(date_string):
    if not date_string:
        return None
    try:
        # Xử lý định dạng %y (năm 2 chữ số, ví dụ 25 cho 2025)
        input_format = '%d/%m/%y'
        output_format = '%Y-%m-%d'
        date_object = datetime.strptime(date_string.strip(), input_format)
        return date_object.strftime(output_format)
    except ValueError:
        return None