import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_hdg'
    mcpcty = 'HDG'
    allowed_domains = ['hado.com.vn'] 
    start_urls = ['https://hado.com.vn/quan-he-co-dong?t=18'] 

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

        # 2. Lấy tất cả các hàng dữ liệu (<tr>) trong phần tbody
        rows = response.css('table.table-bordered tbody tr')

        for row in rows:
            # Lấy thông tin từ các cột
            title_element = row.css('td:nth-child(2) a')
            title_raw = title_element.css('::text').get()
            data_id = title_element.css('::attr(data-id)').get() # ID nội bộ của web
            date_pub_raw = row.css('td:nth-child(3) p::text').get()

            if not title_raw:
                continue

            cleaned_title = title_raw.strip()
            iso_date = convert_date_to_iso8601(date_pub_raw)
            
            # Ghi chú: Website HDG thường mở popup hoặc tải file qua data-id 
            # Ở đây ta lưu info cơ bản, nếu cần link cụ thể có thể bổ sung logic click/request
            
            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Sử dụng data_id của web nếu có để làm ID duy nhất, nếu không dùng title+date
            uid = data_id if data_id else f"{cleaned_title}_{iso_date}"
            event_id = uid.replace(' ', '_').strip()[:150]
            
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
            e_item['details_raw'] = f"{cleaned_title} (ID: {data_id})"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    input_format = "%d/%m/%Y"    
    output_format = '%Y-%m-%d'
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        return date_object.strftime(output_format)
    except ValueError:
        return None