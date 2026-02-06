import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_tpb'
    mcpcty = 'TPB'
    allowed_domains = ['tpb.vn'] 
    start_urls = ['https://tpb.vn/nha-dau-tu/thong-bao-co-dong','https://tpb.vn/nha-dau-tu/bao-cao-tai-chinh'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                # TPBank yêu cầu Playwright để render nội dung động
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

        # 2. Lấy danh sách các khối download
        items = response.css('.b-right-download')

        for item in items:
            file_url = item.css('a::attr(href)').get()
            raw_text = item.css('span::text').get()
            
            if not raw_text:
                continue

            # Làm sạch khoảng trắng và xuống dòng
            clean_text = " ".join(raw_text.split())
            
            # Tách ngày (10 ký tự đầu) và tiêu đề (phần còn lại)
            publish_date = clean_text[:10] 
            title = clean_text[10:].strip()
            
            iso_date = convert_date_to_iso8601(publish_date)
            summary = title if title else "Thông báo cổ đông"

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
            
            full_file_link = response.urljoin(file_url) if file_url else "N/A"
            e_item['details_raw'] = f"{summary}\nLink file: {full_file_link}"
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