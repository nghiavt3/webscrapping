import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_gda'
    mcpcty = 'GDA'
    allowed_domains = ['tondonga.com.vn'] 
    start_urls = ['https://www.tondonga.com.vn/thong-tin-nha-dau-tu/cong-bo-thong-tin',
                  'https://www.tondonga.com.vn/thong-tin-nha-dau-tu/bao-cao-tai-chinh'] 

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
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Duyệt qua từng dòng trong bảng dữ liệu
        rows = response.css('.uk-table-responsive tbody tr')
        
        for row in rows:
            # Trích xuất tiêu đề và ngày đăng dựa trên thuộc tính title của td
            title_raw = row.css('td[title="Tài liệu"] a::text').get()
            date_raw = row.css('td[title="Ngày đăng"]::text').get()
            download_path = row.css('a.download::attr(href)').get()
            
            if not title_raw:
                continue

            cleaned_title = title_raw.strip()
            iso_date = convert_date_to_iso8601(date_raw)
            full_download_url = response.urljoin(download_path) if download_path else None

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{cleaned_title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{cleaned_title}\nLink: {full_download_url}"
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