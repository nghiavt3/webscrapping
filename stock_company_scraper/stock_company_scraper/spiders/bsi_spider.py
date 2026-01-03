import scrapy
import sqlite3
import os
from stock_company_scraper.items import EventItem
from datetime import datetime
import re

class EventSpider(scrapy.Spider):
    name = 'event_bsi'
    mcpcty = 'BSI'
    allowed_domains = ['bsc.com.vn'] 
    start_urls = ['https://www.bsc.com.vn/quan-he-co-dong/'] 

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

        # 2. Lấy tất cả các item tin tức trong khối Công bố thông tin
        items = response.css('.news_service-item')

        for item in items:
            # Trích xuất dữ liệu cơ bản
            title = (item.css('.main_title::text').get() or "").strip()
            summary_text = (item.css('.main_content::text').get() or "").strip()
            pdf_link = item.css('::attr(data-doccument)').get()

            # Trích xuất ngày tháng (Ngày, Tháng, Năm nằm ở các thẻ khác nhau)
            month_text = item.css('.date::text').get() # Ví dụ: "Tháng 12"
            day = item.xpath('.//p[contains(@class, "flex-1")]/text()').get()
            year = item.css('span.text-primary-300::text').get()

            # Chuẩn hóa chuỗi ngày để convert
            full_date_raw = f"{day.strip() if day else ''} {month_text.strip() if month_text else ''} {year.strip() if year else ''}"
            iso_date = convert_vi_date_to_iso(full_date_raw)
            
            # Xử lý link PDF đầy đủ
            full_pdf_url = response.urljoin(pdf_link) if pdf_link else ""

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{title}_{iso_date}".replace('/', '-').replace('.', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT GIA TĂNG.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = f"{title}\nNội dung: {summary_text}\nLink PDF: {full_pdf_url}"
            e_item['date'] = iso_date 
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_vi_date_to_iso(date_str):
    """
    Xử lý: '25 Tháng 12 2024' -> '2024-12-25'
    """
    if not date_str:
        return None
        
    # Làm sạch chuỗi
    date_str = date_str.replace(',', '').lower().strip()
    
    month_mapping = {
        'tháng 1': '01', 'tháng 2': '02', 'tháng 3': '03', 'tháng 4': '04',
        'tháng 5': '05', 'tháng 6': '06', 'tháng 7': '07', 'tháng 8': '08',
        'tháng 9': '09', 'tháng 10': '10', 'tháng 11': '11', 'tháng 12': '12'
    }
    
    # Thay thế tên tháng tiếng Việt bằng số
    for vi_month, num_month in month_mapping.items():
        if vi_month in date_str:
            date_str = date_str.replace(vi_month, num_month)
            break
            
    try:
        # Sau khi thay thế, date_str có dạng: "25 01 2024"
        parts = date_str.split()
        if len(parts) == 3:
            day = parts[0].zfill(2)
            month = parts[1]
            year = parts[2]
            return f"{year}-{month}-{day}"
    except Exception:
        pass
    return None