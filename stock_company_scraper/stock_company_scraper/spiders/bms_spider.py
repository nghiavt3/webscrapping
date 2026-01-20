import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import re

class EventSpider(scrapy.Spider):
    name = 'event_bms'
    mcpcty = 'BMS'
    allowed_domains = ['bmsc.com.vn'] 
    start_urls = ['https://bmsc.com.vn/tin-co-dong/']

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

        # 2. Duyệt qua danh sách tin tức
        items = response.css('.viewcat_list .item')
        
        for item in items:
            # Trích xuất dữ liệu
            title = item.css('a::attr(title)').get()
            link = item.css('a::attr(href)').get()
            
            # Trích xuất ngày đăng (xử lý getall() để nối chuỗi văn bản)
            date_raw_list = item.css('.text-muted li:nth-child(1)::text').getall()
            date_raw_str = "".join(date_raw_list).strip() if date_raw_list else None
            
            # Làm sạch dữ liệu
            clean_title = title.strip() if title else ""
            iso_date = convert_date_to_iso8601(date_raw_str)
            full_link = response.urljoin(link)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{clean_title}_{iso_date}".replace('/', '-').replace('.', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{clean_title}]. DỪNG QUÉT GIA TĂNG.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = clean_title
            e_item['details_raw'] = f"{clean_title}\nLink: {full_link}"
            e_item['date'] = iso_date 
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    """
    Xử lý: '29/12/2025 02:30:15 PM' -> '2025-12-29'
    Hỗ trợ cả trường hợp chỉ có ngày DD/MM/YYYY
    """
    if not vietnam_date_str:
        return None

    clean_str = vietnam_date_str.strip()
    output_format = '%Y-%m-%d'

    # Thử định dạng đầy đủ (Ngày Giờ AM/PM)
    try:
        date_object = datetime.strptime(clean_str, '%d/%m/%Y %I:%M:%S %p')
        return date_object.strftime(output_format)
    except ValueError:
        # Thử định dạng chỉ có ngày
        try:
            date_object = datetime.strptime(clean_str, '%d/%m/%Y')
            return date_object.strftime(output_format)
        except ValueError:
            return clean_str