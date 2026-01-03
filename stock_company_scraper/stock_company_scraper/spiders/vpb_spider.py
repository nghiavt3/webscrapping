import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_vpb'
    mcpcty = 'VPB'
    allowed_domains = ['vpbank.com.vn'] 
    start_urls = ['https://www.vpbank.com.vn/quan-he-nha-dau-tu/cong-bo-thong-tin-khac'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        yield scrapy.Request(
            url=self.start_urls[0],
            callback=self.parse,
            meta={"playwright": True}
        )

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

        # 2. Lấy tất cả các khối tin nhắn công bố
        announcements = response.css('div.shadow-sm')

        for item in announcements:
            title = item.css('h3::text').get()
            raw_date = item.css('p.text-gray-400::text').get()
            file_url = item.css('ul li a::attr(href)').get()

            if not title or not raw_date:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso8601(raw_date.strip())

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
            
            full_file_url = response.urljoin(file_url) if file_url else "N/A"
            e_item['details_raw'] = f"{summary}\nLink: {full_file_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    # VPB format: "HH:MM DD/MM/YYYY"
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), "%H:%M %d/%m/%Y")
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        # Dự phòng nếu format chỉ có ngày
        try:
            return datetime.strptime(vietnam_date_str.strip()[-10:], "%d/%m/%Y").strftime('%Y-%m-%d')
        except:
            return None