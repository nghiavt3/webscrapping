import scrapy
import sqlite3
import re
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_hax'
    mcpcty = 'HAX'
    allowed_domains = ['haxaco.com.vn'] 
    # Cào chuyên mục Đại hội đồng cổ đông
    start_urls = ['https://www.haxaco.com.vn/dai-hoi-dong-co-dong/2025-98/'] 

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
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Chọn tất cả các dòng trong thân bảng
        rows = response.css('div.main.newdautu table tbody tr')
        
        for row in rows:
            # Trích xuất tiêu đề
            title = row.css('td:nth-child(1)::text').get()
            
            # Trích xuất ngày tháng (HAX để ngày và giờ tách biệt trong tập hợp text nodes)
            raw_date_list = row.css('td:nth-child(2)::text').getall()
            date_str = raw_date_list[0].strip() if raw_date_list else None
            
            # Trích xuất Link từ chuỗi Javascript onclick bằng Regex
            onclick_text = row.css('td:nth-child(4) a::attr(onclick)').get()
            pdf_link = None
            if onclick_text:
                # Tìm URL bắt đầu bằng https bên trong dấu nháy đơn
                match = re.search(r"'(https://[^']+)'", onclick_text)
                if match:
                    pdf_link = match.group(1)

            if not title:
                continue

            cleaned_title = title.strip()
            iso_date = convert_date_to_iso8601(date_str)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{cleaned_title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{cleaned_title}\nLink: {pdf_link}"
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