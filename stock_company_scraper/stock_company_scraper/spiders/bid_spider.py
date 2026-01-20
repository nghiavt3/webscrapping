import scrapy
import sqlite3
import os
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_bid'
    mcpcty = 'BID'
    allowed_domains = ['bidv.com.vn'] 
    start_urls = ['https://bidv.com.vn/vn/quan-he-nha-dau-tu/thong-tin-co-dong/'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        # Đường dẫn file db đồng bộ với dự án
        self.db_path = 'stock_events.db'

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={
                    "playwright": True,
                    # Đợi cho khối danh sách tin hiện ra để tránh cào hụt dữ liệu
                    "playwright_page_methods": [
                        {"method": "wait_for_selector", "args": ["div.row.g-2rem"]},
                    ],
                }
            )
        
    def parse(self, response):
        # 1. Kết nối SQLite và chuẩn bị bảng
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

        # 2. Lấy tất cả các thẻ <a> chứa item (khối tin tức của BIDV)
        items = response.css('div.row.g-2rem > div > a')

        for a_tag in items:
            raw_date = a_tag.css('p::text').get()
            title = a_tag.css('h5::text').get()
            link = a_tag.css('::attr(href)').get()

            # Làm sạch và chuẩn hóa dữ liệu
            clean_title = title.strip() if title else ""
            iso_date = format_date(raw_date)
            full_url = response.urljoin(link) if link else ""

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Tạo ID duy nhất (Tiêu đề + Ngày)
            event_id = f"{clean_title}_{iso_date}".replace('/', '-').replace('.', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{clean_title}]. DỪNG QUÉT GIA TĂNG.")
                # Sử dụng break thay vì return để Playwright đóng context êm ái hơn
                break 

            # 4. Yield Item nếu là tin mới
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = clean_title
            e_item['details_raw'] = f"{clean_title}\nLink: {full_url}"
            e_item['date'] = iso_date 
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def format_date(date_str):
    if not date_str:
        return None
    try:
        # BIDV format: DD/MM/YYYY -> ISO: YYYY-MM-DD
        return datetime.strptime(date_str.strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
    except (ValueError, AttributeError):
        return date_str