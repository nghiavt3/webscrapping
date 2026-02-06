import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_tig'
    mcpcty = 'TIG'
    allowed_domains = ['tig.vn'] 
    start_urls = ['https://tig.vn/vi/co-dong/cong-bo-thong-tin-3120/page-1.spp'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def parse(self, response):
        # 1. Khởi tạo SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Lấy danh sách thông báo
        documents = response.css('.right .item')
    
        for doc in documents:
            date_raw = doc.css('.date::text').get()
            title_raw = doc.css('.title::text').get()
            
            if not date_raw or not title_raw:
                continue

            summary = title_raw.strip()
            iso_date = convert_date_to_iso8601(date_raw.strip())
            
            # Xử lý link tải (lấy link đầu tiên hoặc từ select)
            primary_download = doc.css('.attach-file a::attr(href)').get()
            all_files = []
            if primary_download:
                all_files.append(response.urljoin(primary_download))
            
            # Lấy thêm các file phụ nếu có trong thẻ select
            for option in doc.css('.attach-file select option'):
                val = option.css('::attr(value)').get()
                if val and val != "0": # Bỏ qua option mặc định "Chọn file"
                    all_files.append(response.urljoin(val))

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
            
            links_str = "\n".join(set(all_files)) # Dùng set để tránh trùng link
            e_item['details_raw'] = f"{summary}\nFiles:\n{links_str}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        # TIG dùng định dạng DD/MM/YYYY
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None