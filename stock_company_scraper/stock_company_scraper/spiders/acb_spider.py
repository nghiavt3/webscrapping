import scrapy
import sqlite3
import os
import re
from stock_company_scraper.items import EventItem
from datetime import datetime
from scrapy_playwright.page import PageMethod
class EventSpider(scrapy.Spider):
    name = 'event_acb'
    mcpcty = 'ACB'
    allowed_domains = ['acb.com.vn'] 
    start_urls = ['https://acb.com.vn/nha-dau-tu','https://acb.com.vn/nha-dau-tu/bao-cao-tai-chinh'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        # Đường dẫn file db đồng bộ với dự án
        self.db_path = 'stock_events.db'

    async def start(self):
        urls = [
            ('https://acb.com.vn/nha-dau-tu', self.parse),
            ('https://acb.com.vn/nha-dau-tu/bao-cao-tai-chinh', self.parse_generic),
            
             
            
        ]
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                callback=callback,
                meta={'playwright': True,
                      "playwright_page_methods": [
                    # Đợi cho đến khi thẻ div.item-brochure xuất hiện trong DOM
                    PageMethod("wait_for_selector", "div.item-brochure"),
                ],
                      }
            )
        
        
    def parse(self, response):
        # Mở kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Đảm bảo bảng tồn tại theo cấu trúc bạn đã sửa
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

        items = response.css('.item-brochure')

        for item in items:
            raw_text = item.css('.line-2::text').get()
            
            if raw_text:
                raw_text = raw_text.strip()
                
                # 1. Trích xuất và định dạng ngày (YYYY-MM-DD)
                publish_date = convert_date_to_iso8601(raw_text)
                
                # 2. Làm sạch tiêu đề (loại bỏ phần ngày và ngoặc đơn ở cuối)
                title = re.sub(r'\s*\(\d{2}/\d{2}/\d{4}\)$', '', raw_text).strip()

                # -------------------------------------------------------
                # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
                # -------------------------------------------------------
                # Sử dụng quy tắc tạo ID bạn đã hiệu chỉnh
                event_id = f"{title}_{publish_date}".replace('/', '-').replace('.', '_').strip()[:150]
                
                cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
                if cursor.fetchone():
                    self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT GIA TĂNG.")
                    conn.close()
                    break # THOÁT NGAY LẬP TỨC

                # 4. Đưa dữ liệu vào Item nếu là tin mới
                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = title
                e_item['details_raw'] = str(title)
                e_item['date'] = publish_date         
                e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                yield e_item

        conn.close()


    def parse_generic(self, response):
        # Mở kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Đảm bảo bảng tồn tại theo cấu trúc bạn đã sửa
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

        items = response.css('div.item-brochure')

        for item in items:
 
            # 1. Trích xuất và định dạng ngày (YYYY-MM-DD)
            publish_date = None
            
            # 2. Làm sạch tiêu đề (loại bỏ phần ngày và ngoặc đơn ở cuối)
            title = item.css('div.title div.line-2::text').get()
            download_link = item.css('a::attr(href)').get()
            summary = title.strip()
            absolute_url = f"{response.urljoin(download_link)}"
            iso_date = publish_date
            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Sử dụng quy tắc tạo ID bạn đã hiệu chỉnh
            event_id = f"{summary}_{iso_date if iso_date else 'NODATE'}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT GIA TĂNG.")
                conn.close()
                break # THOÁT NGAY LẬP TỨC

            # 4. Đưa dữ liệu vào Item nếu là tin mới
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['details_raw'] = f"{summary}\nLink: {absolute_url} \n"
            e_item['date'] = iso_date         
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

# Hàm xử lý ngày tháng đặc thù cho ACB (Trích xuất từ ngoặc đơn)
def convert_date_to_iso8601(text):
    # Regex tìm định dạng dd/mm/yyyy bên trong dấu ngoặc đơn
    match = re.search(r'\((\d{2}/\d{2}/\d{4})\)', text)
    if match:
        date_str = match.group(1)
        try:
            # Chuyển từ 18/12/2025 -> 2025-12-18
            return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None