import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_vab'
    mcpcty = 'VAB'
    allowed_domains = ['vietabank.com.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        urls = [
            ('https://vietabank.com.vn/nha-dau-tu/thong-bao-thong-tin.html', self.parse_generic),
             ('https://vietabank.com.vn/nha-dau-tu/bao-cao-tai-chinh.html', self.parse_generic),
             ('https://vietabank.com.vn/nha-dau-tu/dai-hoi-co-dong.html', self.parse_generic),
        ]
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                callback=callback,
                #meta={'playwright': True}
            )

    def parse_generic(self, response):
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
        items = response.css('div.list-box a.download-it')

        for item in items:
            # 1. Trích xuất text thô từ thẻ p
            raw_text = item.css('div.r-text p::text').get()
            
            # 2. Xử lý tách Ngày tháng và Tiêu đề bằng Regex
            # Cấu trúc thường gặp: "(Ngày/Tháng/Năm) Nội dung..."
            date = None
            title = raw_text
            
            if raw_text:
                match = re.search(r'\((.*?)\)', raw_text)
                if match:
                    date = match.group(0).strip("()") # Lấy nội dung trong ngoặc đơn (ngày tháng)
                    title = raw_text.replace(match.group(0), '').strip() # Phần còn lại là tiêu đề

            # 3. Trích xuất Link PDF
            file_url = item.css('::attr(href)').get()
            absolute_url = response.urljoin(file_url)
            summary = title
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