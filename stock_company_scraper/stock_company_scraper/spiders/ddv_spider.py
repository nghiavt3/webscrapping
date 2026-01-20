import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_ddv'
    mcpcty = 'DDV'
    allowed_domains = ['dap-vinachem.com.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        urls = [
            ('https://www.dap-vinachem.com.vn/thong-bao-tin-tuc', self.parse),
            ('https://www.dap-vinachem.com.vn/cong-bo-thong-tin', self.parse),
             ('https://www.dap-vinachem.com.vn/nghi-quyet', self.parse),
             ('https://www.dap-vinachem.com.vn/bao-cao-thuong-nien/bao-cao-thuong-nien-nam-2024', self.parse),
             
            
        ]
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                callback=callback,
                #meta={'playwright': True}
            )


    def parse(self, response):
        # 1. Kết nối SQLite và chuẩn bị bảng
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        table_name = f"{self.name}"
        #cursor.execute(f'''DROP TABLE IF EXISTS {table_name}''')
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

        # 2. Chọn tất cả các item tin tức
        news_items = response.css('.list-news.row .item-wrap')

        for item in news_items:
            title = item.css('h5.item-title a::text').get()
            url = item.css('h5.item-title a::attr(href)').get()
            content_summary = item.css('p.content::text').get()
            
            # Trích xuất ngày đăng và làm sạch ký tự xuống dòng
            date_raw = item.css('.item-body .date::text').get()
            # Xử lý chuỗi để lấy ngày, ví dụ: ' 30/07/2025' -> '30/07/2025'
            date = date_raw.replace('\r', '').replace('\n', '').strip() if date_raw else None

            if not title:
                continue

            # Làm sạch và định dạng dữ liệu
            cleaned_title = title.strip()
            iso_date = convert_date_to_iso8601(date)
            full_url = response.urljoin(url)
            scraped_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Tạo ID duy nhất từ Tiêu đề và Ngày
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
            e_item['details_raw'] = f"{cleaned_title}\n{content_summary.strip() if content_summary else ''}\nLink: {full_url}"
            e_item['scraped_at'] = scraped_date
            
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