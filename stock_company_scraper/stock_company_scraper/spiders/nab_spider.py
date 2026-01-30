import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_nab'
    mcpcty = 'NAB'
    allowed_domains = ['namabank.com.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def start(self):
        
        urls = [
            ('https://www.namabank.com.vn/cong-bo-thong-tin', self.parse_generic),
            ('https://www.namabank.com.vn/thong-bao-cua-hdqt', self.parse_generic),
            ('https://www.namabank.com.vn/dai-hoi-co-dong', self.parse_generic),
            ('https://www.namabank.com.vn/2025-3', self.parse_generic),
        ]
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                callback=callback,
                #meta={'playwright': True}
            )

    async def parse_generic(self, response):
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
        items = response.css('div.main-list div.item')

        for item in items:
            # 1. Trích xuất text thô từ thẻ a trong figcaption
            # Lưu ý: Lấy từ thuộc tính title sẽ đầy đủ và sạch hơn text hiển thị
            raw_title = item.css('figcaption a::attr(title)').get()
            
            # 2. Dùng Regex để tách Ngày và Tiêu đề
            # Định dạng mục tiêu: [Đăng ngày 29/03/2025] Nội dung...
            date = None
            clean_title = raw_title
            
            if raw_title:
                # Tìm chuỗi có dạng dd/mm/yyyy
                date_match = re.search(r'(\d{2}/\d{2}/\d{4})', raw_title)
                if date_match:
                    date = date_match.group(1)
                
                # Loại bỏ phần [Đăng ngày ...] để lấy tiêu đề thuần túy
                clean_title = re.sub(r'\[.*?\]', '', raw_title).strip()

            # 3. Trích xuất link PDF
            file_url = item.css('figcaption a::attr(href)').get()

            absolute_url = response.urljoin(file_url)
            summary = clean_title
            iso_date = convert_date_to_iso8601(date)

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