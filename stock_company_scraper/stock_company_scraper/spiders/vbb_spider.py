import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_vbb'
    mcpcty = 'vbb'
    allowed_domains = ['vietbank.com.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'
    
    async def start(self):
        year = datetime.now().year
        month = datetime.now().month
        urls = [
            (f"https://www.vietbank.com.vn/nha-dau-tu/cong-bo-thong-tin?year={year}&category=2&month={month}", self.parse_generic),
            (f"https://www.vietbank.com.vn/nha-dau-tu/cong-bo-thong-tin?year={year}&category=1&month={month}", self.parse_generic),
            (f"https://www.vietbank.com.vn/nha-dau-tu/bao-cao-dinh-ky?category=5&year={year}&quarter=01", self.parse_generic),
            (f"https://www.vietbank.com.vn/nha-dau-tu/bao-cao-dinh-ky?category=5&year={year}&quarter=02", self.parse_generic),
            (f"https://www.vietbank.com.vn/nha-dau-tu/bao-cao-dinh-ky?category=5&year={year}&quarter=03", self.parse_generic),
            (f"https://www.vietbank.com.vn/nha-dau-tu/bao-cao-dinh-ky?category=5&year={year}&quarter=04", self.parse_generic),
            
        ]
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                callback=callback,
                #meta={'playwright': True}
            )

    async def parse_generic(self, response):
        year = datetime.now().year
        month = datetime.now().month
        """Hàm parse dùng chung cho các chuyên mục"""
        # 1. Khởi tạo SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        #cursor.execute(f'''DROP TABLE IF EXISTS {table_name}''')
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # Lấy tất cả các khối bao quanh bản tin
        # Lấy danh sách các item
        # Chọn tất cả các hàng trong thân bảng
        # Duyệt qua từng dòng trong danh sách
        items = response.css('ul.data-list li')

        for item in items:
            # 1. Trích xuất tiêu đề (lấy text bên trong thẻ span và làm sạch khoảng trắng)
            title = item.css('.list-item-codong-title span::text').get()
            if title:
                title = f"{year}/{month}:{title.strip()}"
            # 2. Trích xuất link PDF từ thẻ <object>
            # Lưu ý: Link nằm trong thuộc tính 'data'
            pdf_link = item.css('object.frame-file::attr(data)').get()

            # 3. Trường hợp thẻ object không có, lấy dự phòng ở thẻ <a> trong thẻ <p>
            if not pdf_link:
                pdf_link = item.css('.modal-body p a::attr(href)').get()
    
            
            absolute_url = response.urljoin(pdf_link) if pdf_link else None
            summary = title
            iso_date = None

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            if iso_date :
                event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
            else :
                event_id = f"{summary}_NODATE".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT CHUYÊN MỤC.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{summary}\nLink: {absolute_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None