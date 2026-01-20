import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_dgt'
    mcpcty = 'DGT'
    allowed_domains = ['dgtc.vn'] 
    start_urls = ['https://dgtc.vn/co-dong/thong-tin-co-dong'] 

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

        # 2. Lặp qua từng khối tin tức
        for item in response.css('div.row.gird-padding-custom'):
            # Lấy ngày (VD: '02')
            day = item.css('.gird-padding-custom-day::text').get()
            
            # Lấy tháng-năm (VD: '12-2025')
            month_year_raw = item.css('div.col-gird-padding > div.gird-padding-time::text').get()
            month_year = month_year_raw.strip() if month_year_raw else ""
            
            # Làm sạch dữ liệu và ghép thành DD/MM/YYYY
            day_clean = day.strip() if day else ""
            # Chuyển đổi dấu gạch ngang '-' thành '/' để khớp với hàm convert
            full_date_vn = f"{day_clean}/{month_year.replace('-', '/')}"
            
            # Lấy Tiêu đề và URL
            title = item.css('a.gird-padding-custom-title::text').get()
            url = item.css('a.gird-padding-custom-title::attr(href)').get()
            
            if not title:
                continue

            title_clean = title.strip()
            iso_date = convert_date_to_iso8601(full_date_vn)
            full_url = response.urljoin(url)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{title_clean}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title_clean}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title_clean
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{title_clean}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str or '/' not in vietnam_date_str:
        return None
    # Định dạng đầu vào mong đợi: DD/MM/YYYY
    input_format = '%d/%m/%Y'
    output_format = '%Y-%m-%d'
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        return date_object.strftime(output_format)
    except ValueError:
        return None