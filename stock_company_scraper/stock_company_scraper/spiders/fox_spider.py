import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_fox'
    mcpcty = 'FOX'
    allowed_domains = ['fpt.vn'] 
    
    # Quét cả Thông báo khác và Thông báo trả cổ tức
    start_urls = [
        'https://fpt.vn/vi/ve-fpt-telecom/quan-he-co-dong/thong-bao-khac',
        'https://fpt.vn/vi/ve-fpt-telecom/quan-he-co-dong/thong-bao-tra-co-tuc'
    ] 

    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
    }

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={'playwright': True}
            )

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

        # 2. Nhắm mục tiêu hàng dữ liệu trong container AJAX
        rows = response.css('#ajax-container table.table tbody tr.table-row')
        
        for row in rows:
            title = row.css('td:nth-child(1)::text').get()
            date_time_raw = row.css('td:nth-child(2)::text').get()
            view_link = row.css('td:nth-child(3) a.view-pdf::attr(href)').get()
            download_link = row.css('td:nth-child(4) a.img-download::attr(href)').get()

            date_time_stripped = date_time_raw.strip() if date_time_raw else None
            date_only = None
            time_only = None
            if date_time_stripped and ' ' in date_time_stripped:
                date_only, time_only = date_time_stripped.split(' ', 1)
            else:
                date_only = date_time_stripped
            if not title:
                continue

            cleaned_title = title.strip()
            # Tách ngày từ chuỗi "DD/MM/YYYY HH:MM"
            iso_date = convert_date_to_iso8601(date_only)
            
            # Ưu tiên link xem trực tiếp hoặc tải về
            final_link = response.urljoin(view_link if view_link else download_link)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{cleaned_title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT TRANG NÀY.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{cleaned_title}\nLink tài liệu: {final_link}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d-%m-%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None