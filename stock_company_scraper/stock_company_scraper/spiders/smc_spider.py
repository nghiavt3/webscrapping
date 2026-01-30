import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_smc'
    mcpcty = 'SMC'
    allowed_domains = ['smc.vn'] 
    start_urls = ['https://smc.vn/quan-he-co-dong/'] 

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

        # 2. Chọn các bài đăng trong danh sách blog
        posts = response.css('.blog-posts .post')

        for post in posts:
            # Trích xuất Ngày công bố
            date_raw = post.css('.post-meta .meta-date::text').get()
            clean_date = date_raw.replace('\n', '').strip() if date_raw else None
            
            # Trích xuất Tiêu đề (xử lý trường hợp có thẻ img/icon xen kẽ)
            title_parts = post.css('h2.entry-title a::text').getall()
            summary_text = " ".join([p.strip() for p in title_parts if p.strip()])
            
            # Trích xuất URL và Mô tả ngắn (excerpt)
            link = post.css('h2.entry-title a::attr(href)').get()
            excerpt = post.css('p.post-excerpt::text').get() or ""

            if not summary_text:
                continue

            iso_date = convert_date_to_iso8601(clean_date)
            full_url = response.urljoin(link)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary_text}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{summary_text}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary_text
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{summary_text}\n{excerpt.strip()}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        # SMC dùng định dạng D/M/YYYY (ví dụ 8/12/2025)
        # %d/%m/%Y vẫn xử lý được trường hợp 1 chữ số
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None