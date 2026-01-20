import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_szc'
    mcpcty = 'SZC'
    allowed_domains = ['sonadezichauduc.com.vn'] 
    start_urls = ['https://sonadezichauduc.com.vn/vn/thong-tin-co-dong.html'] 

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

        # 2. Chọn tất cả các mục tin tức
        items = response.css('div.list_download div.item')

        for item in items:
            title = item.css('div.i-title a::text').get()
            article_url = item.css('div.i-title a::attr(href)').get()
            download_url = item.css('div.link_download a::attr(href)').get()
            
            # Xử lý ngày tháng phức tạp
            date_time_list = item.css('div.i-date ::text').getall()
            clean_text_list = [t.strip() for t in date_time_list if t.strip()]
            date_only = None
            
            if clean_text_list:
                # Lấy phần tử cuối chứa 'DD/MM/YYYY, HH:MM AM/PM'
                date_time_raw = clean_text_list[-1]
                date_only = date_time_raw.split(',')[0].strip()

            if not title or not date_only:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso8601(date_only)
            
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
            
            detail_links = []
            if article_url: detail_links.append(f"Link: {response.urljoin(article_url)}")
            if download_url: detail_links.append(f"Download: {response.urljoin(download_url)}")
            
            e_item['details_raw'] = f"{summary}\n" + "\n".join(detail_links)
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        date_object = datetime.strptime(vietnam_date_str, '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None