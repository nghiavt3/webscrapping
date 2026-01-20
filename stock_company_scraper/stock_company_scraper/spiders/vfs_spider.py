import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_vfs'
    mcpcty = 'VFS'
    allowed_domains = ['vfs.com.vn'] 
    start_urls = ['https://www.vfs.com.vn/danh-muc-bao-cao/cong-bo-thong-tin'] 

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

        # 2. Duyệt qua các thẻ div.item
        for report in response.css('div.item'):
            title = report.css('h4 a::text').get()
            url_raw = report.css('h4 a::attr(href)').get()
            date_text = report.css('p.archive-report-date::text').get()
            
            if not title or not date_text:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso8601(date_text.strip())

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
            
            full_url = response.urljoin(url_raw) if url_raw else "N/A"
            e_item['details_raw'] = f"{summary}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    # Định dạng của VFS: '31-12-2025 - 08:30'
    input_format = '%d-%m-%Y - %H:%M'
    output_format = '%Y-%m-%d'
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        return date_object.strftime(output_format)
    except ValueError:
        # Backup trường hợp không có giờ
        try:
            return datetime.strptime(vietnam_date_str.strip()[:10], '%d-%m-%Y').strftime('%Y-%m-%d')
        except:
            return None