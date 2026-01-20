import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_tni'
    mcpcty = 'TNI'
    allowed_domains = ['thanhnamgroup.com.vn'] 
    start_urls = ['https://thanhnamgroup.com.vn/bao-cao/quan-he-co-dong/'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
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

        # 2. Duyệt qua danh sách tin (qhcd_item)
        for item in response.css('div.qhcd_item'):
            title = item.css('h3 a::text').get()
            link = item.css('h3 a::attr(href)').get()
            date_raw = item.css('div.date::text').get()
            excerpt = item.css('div.desc::text').get(default='').strip()
            
            if not title or not date_raw:
                continue

            # Xử lý cắt chuỗi ngày tháng: "Thứ Hai, 20/09/2025" -> "20/09/2025"
            try:
                # Logic của bạn: tách theo dấu phẩy, lấy phần sau, rồi lấy cụm đầu tiên
                clean_date_str = date_raw.split(',')[1].strip().split()[0]
                iso_date = convert_date_to_iso8601(clean_date_str)
            except (IndexError, AttributeError):
                iso_date = datetime.now().strftime('%Y-%m-%d')

            summary = title.strip()

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT.")
                break 

            # 4. Đóng gói Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{summary}\n{excerpt}\nLink: {response.urljoin(link)}"
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