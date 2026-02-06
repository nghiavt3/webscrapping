import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_tci'
    mcpcty = 'TCI'
    allowed_domains = ['tcsc.vn'] 
    # Danh sách các chuyên mục quan trọng của TCI
    start_urls = [
        'https://tcsc.vn/vi/download/Bao-cao-tai-chinh/',
        'https://tcsc.vn/vi/download/Bao-cao-TLATTC-38/',
        'https://tcsc.vn/vi/download/Bao-cao-thuong-nien/',
        'https://tcsc.vn/vi/download/Bao-cao-quan-tri-Cong-ty/',
        'https://tcsc.vn/vi/download/Hop-Dai-hoi-co-dong/'
    ]

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

        # 2. Chọn tất cả các khối div Row (bỏ qua header)
        rows = response.css('div.divTable div.Row')

        for row in rows:
            cells = row.css('div.Cell')
            if len(cells) < 2: # Bỏ qua các hàng không đủ dữ liệu hoặc header
                continue

            # Trích xuất thông tin
            title_raw = cells[0].css('a::attr(title)').get() or cells[0].css('a::text').get()
            file_url = cells[0].css('a::attr(href)').get()
            upload_date = cells[1].css('::text').get()

            if not title_raw or not upload_date:
                continue

            summary = title_raw.strip()
            iso_date = convert_date_to_iso8601(upload_date.strip())
            full_url = response.urljoin(file_url) if file_url else ""

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ TẠI CHUYÊN MỤC NÀY: [{summary}]. CHUYỂN TRANG/DỪNG.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{summary}\nLink tải: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        # TCI sử dụng định dạng "DD/MM/YYYY HH:MM"
        # Hàm strptime sẽ xử lý việc tách lấy ngày
        date_object = datetime.strptime(vietnam_date_str.strip(), "%d/%m/%Y %H:%M")
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        # Dự phòng trường hợp chỉ có ngày
        try:
            date_object = datetime.strptime(vietnam_date_str.strip(), "%d/%m/%Y")
            return date_object.strftime('%Y-%m-%d')
        except:
            return None