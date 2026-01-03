import scrapy
import sqlite3
import os
import re
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_apg'
    mcpcty = 'APG'
    allowed_domains = ['apsi.vn'] 
    # Danh sách các trang quan trọng của APG
    start_urls = [
        'https://apsi.vn/investors/info_fin',
        'https://apsi.vn/investors/share_holders',
        'https://apsi.vn/investors/info_disc'
    ]

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

        items = response.css('div.item--invest')
        
        for item in items:
            # Trích xuất dữ liệu cơ bản
            title = (item.css('.content .title::text').get() or "").strip()
            publish_date_raw = (item.css('.content .publishDate::text').get() or "").strip()
            iso_date = convert_date_to_iso8601(publish_date_raw)

            # 2. Xử lý link từ thuộc tính phx-click (Sử dụng Regex)
            phx_data = item.attrib.get('phx-click', '')
            link = "N/A"
            if phx_data:
                match = re.search(r'"href":"([^"]+)"', phx_data)
                if match:
                    relative_url = match.group(1)
                    link = response.urljoin(relative_url)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{title}_{iso_date}".replace('/', '-').replace('.', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG NHÁNH QUÉT NÀY.")
                # Dùng break để thoát vòng lặp của trang hiện tại một cách an toàn
                break 

            # 4. Đóng gói Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = f"{title}\nLink: {link}"
            e_item['date'] = iso_date 
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item
            
        conn.close()

# Hàm convert dùng chung
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