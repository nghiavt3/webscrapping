import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_nvl'
    mcpcty = 'NVL'
    allowed_domains = ['novaland.com.vn'] 
    start_urls = ['https://www.novaland.com.vn/quan-he-dau-tu/cong-bo-thong-tin/thong-bao'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # 1. Kết nối và khởi tạo bảng SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Chọn các hàng dữ liệu (bỏ qua header)
        data_rows = response.css('div.block-shareHoldersList table.table tbody tr:nth-child(n+2)')
        
        for row in data_rows:
            document_title = row.css('td:nth-child(1) a::text').get()
            issue_date = row.css('td:nth-child(2)::text').get()
            relative_download_url = row.css('td:nth-child(3) a::attr(href)').get()
            
            if not document_title:
                continue

            summary = document_title.strip()
            iso_date = convert_date_to_iso8601(issue_date)
            absolute_url = response.urljoin(relative_download_url) if relative_download_url else ""

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{summary}\nPDF: {absolute_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), "%d/%m/%Y")
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None