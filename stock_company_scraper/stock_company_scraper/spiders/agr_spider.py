import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
from scrapy_playwright.page import PageMethod
import re

class EventSpider(scrapy.Spider):
    name = 'event_agr'
    mcpcty = 'AGR'
    allowed_domains = ['agriseco.com.vn'] 
    start_urls = ['https://agriseco.com.vn/InvestorRelations/IRInGroup/25/vi-VN'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        # Đường dẫn file db khớp với dự án
        self.db_path = 'stock_events.db'

    def start_requests(self):
        yield scrapy.Request(
            url=self.start_urls[0],
            callback=self.parse,
            meta={
                "playwright": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", "div.div-grid-obj"),
                    PageMethod("wait_for_timeout", 1000), 
                ],
            }
        )
        
    def parse(self, response):
        # 1. Mở kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Đảm bảo bảng tồn tại
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

        # 2. Lặp qua danh sách các khối tin tức
        grid_items = response.css('div.div-grid-obj')
        
        for item in grid_items:
            # Trích xuất dữ liệu
            raw_date = item.css('.grid-obj-date::text').getall()
            date_text = "".join(raw_date).strip() if raw_date else None
            # 1. Tìm chuỗi có định dạng dd/mm/yyyy
            date_match = re.search(r'(\d{2}/\d{2}/\d{4})', date_text)
            if date_match:
                date_str = date_match.group(1)
            iso_date = convert_date_to_iso8601(date_str)
            
            # Lưu ý: title ở code cũ của bạn có dấu phẩy dư thừa tạo thành tuple, tôi đã sửa lại
            title = item.css('.grid-obj-title::text').get().strip() if item.css('.grid-obj-title::text') else ""
            description = item.css('.grid-obj-descript::text').get().strip() if item.css('.grid-obj-descript::text') else ""
            link = response.urljoin(item.css('a::attr(href)').get())

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{title}_{iso_date}".replace('/', '-').replace('.', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT GIA TĂNG.")
                # Sử dụng break thay vì return để tránh lỗi 'Event loop is closed' với Playwright
                break 

            # 4. Yield Item nếu là tin mới
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = f"{title}\n{description}\n{link}"
            e_item['date'] = iso_date 
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        # 5. Đóng kết nối an toàn
        conn.close()

# Hàm convert chuẩn của bạn
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