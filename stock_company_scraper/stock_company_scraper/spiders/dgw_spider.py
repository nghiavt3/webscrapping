import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_dgw'
    mcpcty = 'DGW'
    allowed_domains = ['digiworld.com.vn'] 
    start_urls = ['https://digiworld.com.vn/quan-he-nha-dau-tu'] 

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
                id TEXT PRIMARY KEY,
                mcp TEXT,
                date TEXT,
                summary TEXT,
                scraped_at TEXT,
                web_source TEXT,
                details_clean TEXT
            )
        ''')

        # 2. Lặp qua các khối tin tức
        for item in response.css('div.investor-item'):
            # Trích xuất tiêu đề
            title = item.css('.investor-item__title::text').get()
            
            # Trích xuất ngày giờ (xử lý khoảng trắng và &nbsp;)
            raw_datetime = item.css('.investor-item__datetime::text').getall()
            clean_datetime = " ".join([t.strip() for t in raw_datetime if t.strip()])
            
            # Trích xuất link PDF
            pdf_url = item.css('a.investor-item__btn--primary::attr(href)').get()

            if not title:
                continue

            title_clean = title.strip()
            # Lấy phần ngày (bỏ phần giờ nếu có) để convert
            date_part = clean_datetime.split()[0] if clean_datetime else ""
            iso_date = convert_date_to_iso8601(date_part)
            full_pdf_link = response.urljoin(pdf_url) if pdf_url else ""

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{title_clean}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title_clean}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item để Pipeline lưu DB và gửi Telegram
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title_clean
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{title_clean}\nLink tài liệu: {full_pdf_link}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    input_format = '%d/%m/%Y'
    output_format = '%Y-%m-%d'
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        return date_object.strftime(output_format)
    except ValueError:
        return None