import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_dha'
    mcpcty = 'DHA'
    allowed_domains = ['hoaan.com.vn'] 
    # Khởi đầu với năm hiện tại
    start_urls = [f'http://www.hoaan.com.vn/nam-{datetime.now().year}-cn62/'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # 1. Kết nối SQLite và tạo bảng nếu chưa có
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Chọn vùng chứa tin tức (Sử dụng nth-child(2) như bạn đã xác định)
        second_row = response.css('section.container > div.row:nth-child(2)')
        
        if not second_row:
            self.logger.warning("Không tìm thấy vùng dữ liệu chính (div.row:nth-child(2))")
            return

        # 3. Duyệt qua các bài viết nổi bật và danh sách bên trái
        articles = second_row.css('.investor-main, .investor-left')

        for article in articles:
            title = article.css('h4 a::text').get(default='').strip()
            relative_url = article.css('h4 a::attr(href)').get(default='')
            date_raw = article.css('.info .date::text').get(default='').strip()

            if not title:
                continue

            # Làm sạch và định dạng dữ liệu
            full_url = response.urljoin(relative_url)
            iso_date = date_raw

            # -------------------------------------------------------
            # 4. KIỂM TRA TIN CŨ (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT.")
                break 

            # 5. Đóng gói Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{title}\nLink: {full_url}"
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