import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_dtd'
    mcpcty = 'DTD'
    allowed_domains = ['thanhdathanam.vn'] 
    start_urls = ['https://thanhdathanam.vn/quan-he-co-dong/thong-tin-cong-bo/'] 

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

        # 2. Lấy danh sách bài viết
        posts = response.css('div.new-list div.news-post')

        for post in posts:
            title_selector = post.css('h2.title a')
            title = title_selector.css('::text').get()
            url = title_selector.css('::attr(href)').get()
            
            # Ngày đăng (Thẻ span cuối cùng trong div.date-time)
            date_raw = post.css('div.date-time span:last-child::text').get()
            
            # Tóm tắt (Đoạn text đầu tiên trong thẻ p)
            summary_raw = post.css('p::text').get()

            if not title:
                continue

            cleaned_title = title.strip()
            cleaned_summary = summary_raw.strip() if summary_raw else ""
            iso_date = convert_date_to_iso8601(date_raw)
            full_url = response.urljoin(url)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Tạo ID duy nhất dựa trên tiêu đề và ngày
            event_id = f"{cleaned_title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{cleaned_title}\n{cleaned_summary}\nLink: {full_url}"
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