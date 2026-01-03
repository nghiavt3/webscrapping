import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_tv2'
    mcpcty = 'TV2'
    allowed_domains = ['pecc2.com'] 
    start_urls = ['https://pecc2.com/vn/quan-he-dau-tu.html'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                # PECC2 thường yêu cầu JS để hiển thị danh sách tải xuống
                meta={'playwright': True}
            )
        
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

        # 2. Duyệt qua từng mục tin (colgr)
        items = response.css('.vhGripDow .colgr')
        
        for item in items:
            title = item.css('.nameDow a::text').get()
            url = item.css('.nameDow a::attr(href)').get()
            
            # Trích xuất Ngày tách rời
            day_raw = item.css('.dateDow span::text').get()
            month_year_raw = item.css('.dateDow::text').getall() 
            
            if not title or not day_raw or not month_year_raw:
                continue

            # Ghép ngày tháng: span(05) + text(/12/2025)
            try:
                month_year = month_year_raw[-1].strip()
                date_combined = f"{day_raw.strip()}/{month_year.lstrip('/')}"
                iso_date = convert_date_to_iso8601(date_combined)
            except Exception:
                iso_date = datetime.now().strftime('%Y-%m-%d')

            summary = title.strip()

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
            e_item['details_raw'] = f"{summary}\nLink: {response.urljoin(url) if url else 'N/A'}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        # Xử lý trường hợp có dư dấu / hoặc khoảng trắng
        clean_date = vietnam_date_str.replace('//', '/').strip()
        date_object = datetime.strptime(clean_date, '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None