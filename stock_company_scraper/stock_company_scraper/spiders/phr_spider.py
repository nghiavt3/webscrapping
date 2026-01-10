import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_phr'
    mcpcty = 'PHR'
    allowed_domains = ['phr.vn'] 
    start_urls = ['https://www.phr.vn/thong-tin-co-dong.aspx?catid=6'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                # Kích hoạt Playwright cho trang render JS
                meta={'playwright': True}
            )

    def parse(self, response):
        # 1. Kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''DROP TABLE IF EXISTS {table_name}''')
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Lấy danh sách các khối tin tức
        news_blocks = response.css('div.news-stock-list > div[class^="bc-list"]')
        
        for block in news_blocks:
            detail_link_tag = block.css('a:first-child')
            title = detail_link_tag.css('::text').get()
            
            # Khối chứa các liên kết tải về/xem
            download_container = block.css('div.bc-list-down-in')
            view_url = download_container.css('a:nth-child(1)::attr(href)').get()
            download_url = download_container.css('span a:nth-child(1)::attr(href)').get()
            match = re.search(r'(\d{8})(?=\.pdf)', download_url)
            if match:
                date_str = match.group(1)
                date_obj = convert_date_to_iso8601(date_str)
                iso_date = date_obj
            else:
                iso_date = None
            if not title:
                continue

            summary = title.strip()
            # Vì PHR không hiện ngày ở list, ta dùng ngày quét hoặc parse từ link nếu có
            # Ở đây tôi để mặc định ngày hiện tại hoặc bạn có thể vào page detail để lấy
            full_view_url = response.urljoin(view_url)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_NODATE".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. BỎ QUA.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{summary}\nView PDF: {full_view_url}\nDownload: {response.urljoin(download_url)}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()
    
def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d%m%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None