import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_mcm'
    mcpcty = 'MCM'
    allowed_domains = ['mcmilk.com.vn'] 
    start_urls = ['https://www.mcmilk.com.vn/quan-he-co-dong/cong-bo-thong-tin-khac/'] 

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

        # 2. Chọn tất cả các thẻ article
        articles = response.css('article.post-entry')

        for article in articles:
            title_raw = article.css('h2.post-title a::text').get()
            if not title_raw:
                continue
                
            summary = title_raw.strip()
            
            # Khai thác dữ liệu cấu trúc SEO để lấy ngày sạch
            iso_date_full = article.css('span.av-structured-data[itemprop="datePublished"]::text').get()
            iso_date = iso_date_full[:10] if iso_date_full else datetime.now().strftime('%Y-%m-%d')
            
            post_link = response.urljoin(article.css('h2.post-title a::attr(href)').get())
            pdf_link = response.urljoin(article.css('div.entry-content p a::attr(href)').get())

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
            e_item['details_raw'] = f"Title: {summary}\nPost: {post_link}\nPDF: {pdf_link}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()