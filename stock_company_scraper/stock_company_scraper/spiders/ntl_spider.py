import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_ntl'
    mcpcty = 'NTL'
    allowed_domains = ['lideco.vn'] 
    start_urls = ['https://lideco.vn/chuyen-muc/quan-he-co-dong/'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # 1. Kết nối và khởi tạo SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Lấy danh sách các bài viết (Elementor Custom Skin Loop)
        posts = response.css('article.ecs-post-loop')
        
        for post in posts:
            title_selector = post.css('.elementor-heading-title a')
            title = title_selector.css('::text').get()
            detail_url = title_selector.css('::attr(href)').get() 
            
            # Trích xuất Ngày công bố (Dùng itemprop để tăng độ chính xác)
            date_published = post.css('li[itemprop="datePublished"] span.elementor-post-info__item--type-date::text').get()
            
            # Trích xuất URL PDF nếu có
            download_url = post.css('.elementor-image a::attr(href)').get()
            
            if not title:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso8601(date_published)
            
            # Làm sạch URL
            detail_url_str = response.urljoin(detail_url) if detail_url else ""
            download_url_str = response.urljoin(download_url) if download_url else ""

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
            e_item['details_raw'] = f"Title: {summary}\nDetail: {detail_url_str}\nPDF: {download_url_str}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), "%d/%m/%Y")
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None