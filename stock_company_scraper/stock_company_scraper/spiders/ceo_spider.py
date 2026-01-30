import scrapy
import sqlite3
import os
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_ceo'
    mcpcty = 'CEO'
    allowed_domains = ['ceogroup.com.vn'] 
    start_urls = ['https://ceogroup.com.vn/cong-bo-thong-tin-sc81'] 

    async def start(self):
        urls = [
            ('https://ceogroup.com.vn/cong-bo-thong-tin-sc81', self.parse),
            ('https://ceogroup.com.vn/thong-tin-tai-chinh-sc60', self.parse_bctc),
            ('https://ceogroup.com.vn/tin-tuc-va-su-kien-sc58', self.parse_tintuc),
        ]
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                callback=callback,
               # meta={'playwright': True}
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
                id TEXT PRIMARY KEY,
                mcp TEXT,
                date TEXT,
                summary TEXT,
                scraped_at TEXT,
                web_source TEXT,
                details_clean TEXT
            )
        ''')

        # 2. Lấy danh sách tất cả các hàng dữ liệu (records)
        records = response.css('div.report-item table tbody tr')
        
        for record in records:
            # Trích xuất dữ liệu thô
            title_raw = record.css('td:nth-child(1) h3.title a::text').get()
            doc_url = record.css('td:nth-child(1) h3.title a::attr(href)').get()
            date_posted = record.css('td:nth-child(2)::text').get()
            download_url = record.css('td:nth-child(3) a::attr(href)').get()
            
            # Làm sạch dữ liệu
            cleaned_title = title_raw.strip() if title_raw else ""
            cleaned_date = date_posted.strip() if date_posted else ""
            iso_date = convert_date_to_iso8601(cleaned_date)
            
            # Xử lý link tuyệt đối
            full_doc_url = response.urljoin(doc_url)
            full_dl_url = response.urljoin(download_url)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Tạo ID duy nhất từ Title + Date
            event_id = f"{cleaned_title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT GIA TĂNG.")
                break 

            # 4. Yield Item nếu là tin mới
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{cleaned_title}\nXem: {full_doc_url}\nTải: {full_dl_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

    def parse_bctc(self, response):
        # 1. Kết nối SQLite và chuẩn bị bảng
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
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

        # 2. Lấy danh sách tất cả các hàng dữ liệu (records)
        tables = response.css('div.report-item table')
        
        for table in tables:
            period = table.css('thead th:first-child::text').get()
            rows = table.css('tbody tr')
            for row in rows:
                title_link = row.css('td h3.title a')

                # Trích xuất dữ liệu thô
                title_raw = title_link.css('::text').get().strip()
                doc_url = title_link.css('::attr(href)').get()
                date_posted = row.css('td.text-center:nth-child(2)::text').get()
            
            
                # Làm sạch dữ liệu
                cleaned_title = title_raw.strip() if title_raw else ""
                cleaned_date = date_posted.strip() if date_posted else ""
                iso_date = convert_date_to_iso8601(cleaned_date)
                
                # Xử lý link tuyệt đối
                full_doc_url = response.urljoin(doc_url)
                

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Tạo ID duy nhất từ Title + Date
            event_id = f"{cleaned_title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT GIA TĂNG.")
                break 

            # 4. Yield Item nếu là tin mới
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{cleaned_title}\nXem: {full_doc_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

    def parse_tintuc(self, response):
        # 1. Kết nối SQLite và chuẩn bị bảng
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
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

        # 2. Lấy danh sách tất cả các hàng dữ liệu (records)
        posts = response.css('div.nm-post')
        
        for post in posts:
            # Trích xuất dữ liệu thô
            title_raw = post.css('h2.post-title a::text').get()
            doc_url = post.css('h2.post-title a::attr(href)').get()
            date_posted = post.css('time.post-date::text').get()
            
            
            # Làm sạch dữ liệu
            cleaned_title = title_raw.strip() if title_raw else ""
            cleaned_date = date_posted.strip() if date_posted else ""
            iso_date = convert_date_to_iso8601(cleaned_date)
            
            # Xử lý link tuyệt đối
            full_doc_url = response.urljoin(doc_url)
           

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Tạo ID duy nhất từ Title + Date
            event_id = f"{cleaned_title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT GIA TĂNG.")
                break 

            # 4. Yield Item nếu là tin mới
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{cleaned_title}\nXem: {full_doc_url}\n"
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
        return vietnam_date_str