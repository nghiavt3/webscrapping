import scrapy
import sqlite3
import os
from stock_company_scraper.items import EventItem
from datetime import datetime
import re

class EventSpider(scrapy.Spider):
    name = 'event_bcm'
    mcpcty = 'BCM'
    allowed_domains = ['becamex.com.vn'] 
    start_urls = ['https://becamex.com.vn/quan-he-co-dong/cong-bo-thong-tin/',
                  'https://becamex.com.vn/quan-he-co-dong/bao-cao-tai-chinh/',
                  'https://becamex.com.vn/quan-he-co-dong/dai-hoi-dong-co-dong/'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
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

        # 2. Chọn tất cả các khối tin tức
        news_blocks = response.css('div#shareholder-list .shareholder-item')
        
        for block in news_blocks:
            # 3. Trích xuất Ngày và Tiêu đề
            date_raw = block.css('p.text-primary-1::text').get()
            title_link_tag = block.css('h2 a')
            tieu_de = title_link_tag.css('::text').get()
            pdf_url = title_link_tag.css('::attr(href)').get()

            if not date_raw or not tieu_de:
                continue

            # Làm sạch dữ liệu
            date_raw = date_raw.strip()
            tieu_de = tieu_de.strip()
            full_pdf_url = response.urljoin(pdf_url)

            # Chuẩn hóa ngày sang ISO (YYYY-MM-DD)
            date_iso = normalize_vietnamese_date_ultimate(date_raw)

            # -------------------------------------------------------
            # 4. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{tieu_de}_{date_iso}".replace('/', '-').replace('.', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{tieu_de}]. DỪNG QUÉT GIA TĂNG.")
                break 

            # 5. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = tieu_de
            e_item['details_raw'] = f"{tieu_de}\nLink PDF: {full_pdf_url}"
            e_item['date'] = date_iso
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def normalize_vietnamese_date_ultimate(date_str):
    """
    Xử lý: '02 Tháng 12, 2025' -> '2025-12-02'
    """
    if not date_str:
        return None

    month_mapping = {
        'Tháng 1': '01', 'Tháng 2': '02', 'Tháng 3': '03', 'Tháng 4': '04', 
        'Tháng 5': '05', 'Tháng 6': '06', 'Tháng 7': '07', 'Tháng 8': '08', 
        'Tháng 9': '09', 'Tháng 10': '10', 'Tháng 11': '11', 'Tháng 12': '12'
    }
    
    try:
        # Làm sạch ký tự đặc biệt \xa0 và dấu phẩy
        temp = date_str.replace('\xa0', ' ').replace(',', '').strip()
        parts = temp.split()
        
        # Cấu trúc mong đợi: ['02', 'Tháng', '12', '2025']
        if len(parts) >= 4:
            day = parts[0].zfill(2)
            full_month_name = f"{parts[1]} {parts[2]}"
            year = parts[3]
            
            month_iso = month_mapping.get(full_month_name, "01")
            return f"{year}-{month_iso}-{day}"
    except Exception:
        pass
    return date_str # Trả về gốc nếu lỗi