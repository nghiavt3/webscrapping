import scrapy
import sqlite3
import os
from stock_company_scraper.items import EventItem
from datetime import datetime
import re

class EventSpider(scrapy.Spider):
    name = 'event_abb'
    mcpcty = 'ABB'
    allowed_domains = ['abbank.vn'] 
    start_urls = ['https://abbank.vn/thong-tin/tin-tuc-co-dong'] 

    def start_requests(self):
        urls = [
            ('https://abbank.vn/thong-tin/tin-tuc-co-dong', self.parse),
            ('https://abbank.vn/thong-tin/bao-cao-tai-chinh.html', self.parse_generic),
            ('https://abbank.vn/thong-tin/dai-hoi-dong-co-dong.html', self.parse_generic),
        ]
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                callback=callback,
               # meta={'playwright': True}
            )

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        # Đường dẫn file db khớp với cấu trúc dự án của bạn
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # Mở kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Đảm bảo bảng tồn tại (Cấu trúc chuẩn bạn đã hiệu chỉnh)
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

        # 1. Lấy danh sách tất cả các thông báo (records)
        news_items = response.css('div.main-content ul.list > li')
        
        for item in news_items:
            # 2. Trích xuất Tiêu đề, URL và Ngày đăng
            title = item.css('h3.title a::text').get()
            url = item.css('h3.title a::attr(href)').get()
            
            # Trích xuất Ngày đăng
            date_text = item.css('p.date::text').getall()
            raw_date = date_text[-1].strip() if date_text else None
            
            # Làm sạch dữ liệu để tạo ID
            title_clean = title.strip() if title else ""
            iso_date = convert_date_to_iso8601(raw_date)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Tạo ID duy nhất theo quy tắc chuẩn của bạn
            event_id = f"{title_clean}_{iso_date}".replace('/', '-').replace('.', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title_clean}]. DỪNG QUÉT GIA TĂNG.")
                conn.close()
                break # THOÁT NGAY LẬP TỨC

            # 4. Đưa dữ liệu vào Item nếu là tin mới
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title_clean
            e_item['details_raw'] = f"{title_clean}\nLink: {response.urljoin(url) if url else 'N/A'}"
            e_item['date'] = iso_date 
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()
    
    def parse_generic(self, response):
        # Mở kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Đảm bảo bảng tồn tại (Cấu trúc chuẩn bạn đã hiệu chỉnh)
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

        # 1. Lấy danh sách tất cả các thông báo (records)
        # Tìm tất cả các khối nội dung chính
        main_div = response.css('div.list')
    
        # Lấy danh sách các năm (nằm trong thẻ strong)
        years = main_div.css('strong::text').getall()
        # Lấy các danh sách ul tương ứng với từng năm
        ul_lists = main_div.css('ul')

        for year, ul in zip(years, ul_lists):
            current_year = year.strip()
            # Lặp qua từng dòng báo cáo trong năm đó
            for li in ul.css('li'):
                title = "".join(li.css('a ::text').getall()).strip()
                url = li.css('a::attr(href)').get()
            
            
                # Làm sạch dữ liệu để tạo ID
                title_clean = title.strip() if title else ""
                iso_date = None

                # -------------------------------------------------------
                # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
                # -------------------------------------------------------
                # Tạo ID duy nhất theo quy tắc chuẩn của bạn
                event_id = f"{title_clean}_{iso_date}".replace('/', '-').replace('.', '_').strip()[:150]
                
                cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
                if cursor.fetchone():
                    self.logger.info(f"===> GẶP TIN CŨ: [{title_clean}]. DỪNG QUÉT GIA TĂNG.")
                    conn.close()
                    break # THOÁT NGAY LẬP TỨC

                # 4. Đưa dữ liệu vào Item nếu là tin mới
                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = title_clean
                e_item['details_raw'] = f"{title_clean}\nLink: {response.urljoin(url) if url else 'N/A'}"
                e_item['date'] = iso_date 
                e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                yield e_item

        conn.close()

# Giữ nguyên hàm convert chuẩn của bạn
def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None

    input_format = '%d/%m/%Y'
    output_format = '%Y-%m-%d'

    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        iso_date_str = date_object.strftime(output_format)
        return iso_date_str
    except ValueError as e:
        return None