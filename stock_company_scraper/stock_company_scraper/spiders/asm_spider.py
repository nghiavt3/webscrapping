import scrapy
import sqlite3
import os
from stock_company_scraper.items import EventItem
from datetime import datetime
import re

class EventSpider(scrapy.Spider):
    name = 'event_asm'
    mcpcty = 'ASM'
    allowed_domains = ['saomaigroup.com'] 
    start_urls = ['https://saomaigroup.com/vn/cong-bo-thong-tin.html','https://saomaigroup.com/vn/bao-cao-tai-chinh.html','https://saomaigroup.com/vn/dai-hoi-dong-co-dong.html'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        # Đường dẫn file db khớp với dự án
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # 1. Kết nối SQLite
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

        # 2. Duyệt qua từng dòng tin tức
        items = response.css('div.itholdermm')
        
        for item in items:
            raw_title = item.css('div.mmtitle a::text').get()
            raw_date = item.css('div.mmdate::text').get()
            raw_link = item.css('div.mmtitle a::attr(href)').get()

            # Làm sạch dữ liệu thô
            title = raw_title.strip() if raw_title else ""
            full_link = response.urljoin(raw_link) if raw_link else ""
            
            # Xử lý ngày tháng (Hỗ trợ cả dấu chấm '.' của Sao Mai)
            iso_date = convert_date_to_iso8601(raw_date)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{title}_{iso_date}".replace('/', '-').replace('.', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT GIA TĂNG.")
                break # Thoát vòng lặp nhẹ nhàng

            # 4. Yield Item nếu là tin mới
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = f"{title}\nLink: {full_link}"
            e_item['date'] = iso_date 
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    """
    Hỗ trợ cả định dạng DD/MM/YYYY và DD.MM.YYYY
    """
    if not vietnam_date_str:
        return None

    clean_date = vietnam_date_str.strip()
    # Thay thế dấu chấm thành dấu gạch chéo để đồng nhất xử lý
    clean_date = clean_date.replace('.', '/')
    
    input_format = '%d/%m/%Y'
    output_format = '%Y-%m-%d'

    try:
        date_object = datetime.strptime(clean_date, input_format)
        return date_object.strftime(output_format)
    except ValueError:
        return None