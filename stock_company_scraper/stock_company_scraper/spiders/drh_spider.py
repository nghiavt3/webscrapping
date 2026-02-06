import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_drh'
    mcpcty = 'DRH'
    allowed_domains = ['drh.vn'] 
    start_urls = ['https://drh.vn/quan-he-co-dong.html'] 

    async def start(self):
        """Gửi request đến API với header mô phỏng trình duyệt."""
        
        yield scrapy.Request(
            url='https://drh.vn/quan-he-co-dong/page-1.html?_=1769227600255',
            headers={
            # Header quan trọng để server nhận diện đây là cuộc gọi XHR
            'X-Requested-With': 'XMLHttpRequest',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        },
            callback=self.parse
        )

        yield scrapy.Request(
            url=self.start_urls[0],
            callback=self.parse
        )



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

        # 2. Lấy danh sách thông báo
        announcements = response.css('.list-box')

        for item in announcements:
            date_raw = item.css('.r-date::text').get()
            content = item.css('.r-text p::text').get()
            file_url_relative = item.css('.r-link a::attr(href)').get()

            if not content:
                continue

            # Làm sạch dữ liệu
            cleaned_content = content.strip()
            iso_date = convert_date_to_iso8601(date_raw)
            # Tạo link tuyệt đối cho file (DRH thường dùng link PDF trực tiếp)
            full_file_url = response.urljoin(file_url_relative)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # event_id = f"{cleaned_content}_{iso_date}".replace(' ', '_').strip()[:150]
            
            # cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            # if cursor.fetchone():
            #     self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_content}]. DỪNG QUÉT.")
            #     break 

            # 4. Yield Item cho Pipeline
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_content
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{cleaned_content}\nTài liệu: {full_file_url}"
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