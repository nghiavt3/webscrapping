import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_l14'
    mcpcty = 'L14'
    allowed_domains = ['licogi14.vn'] 
    start_urls = ['https://licogi14.vn/danh-muc/quan-he-co-dong/'] 

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

        posts = response.css('div.col.post-item') 

        for post in posts:
            link = post.css('a.plain::attr(href)').get()
            title = post.css('h5.post-title.is-large::text').get()
            excerpt = post.css('p.from_the_blog_excerpt::text').get()
            
            if not title:
                continue

            summary = title.strip()
            # Vì trang danh mục L14 thiếu ngày rõ ràng, ta tạm lấy ngày hiện tại 
            # hoặc bạn có thể parse từ link nếu link có định dạng date-based
            iso_date = datetime.now().strftime('%Y-%m-%d') 
            
            full_link = response.urljoin(link)

            # -------------------------------------------------------
            # 2. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Do thiếu ngày cố định, ta dùng title làm ID chính
            event_id = f"{summary}_NODATE".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT.")
                break 

            # 3. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['date'] = None
            e_item['details_raw'] = f"Title: {summary}\nExcerpt: {excerpt}\nLink: {full_link}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()