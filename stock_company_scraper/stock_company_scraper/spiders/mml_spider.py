import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_mml'
    mcpcty = 'MML'
    allowed_domains = ['masanmeatlife.com.vn'] 
    start_urls = ['https://masanmeatlife.com.vn/category-shareholder/thong-bao-cong-ty/?lang=vi',
                'https://masanmeatlife.com.vn/category-shareholder/bao-cao-tai-chinh/?lang=vi'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def parse(self, response):
        # 1. Kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Duyệt qua các item tin tức
        for item in response.css('li.shareholder-item'):
            title = item.css('.item-shareholder__name span.fs-13::text').get()
            date_raw = item.css('.item-shareholder__time p:nth-child(1)::text').get()
            # time_raw = item.css('div.dated p:nth-of-type(2)::text').get() # Có thể dùng nếu cần độ chính xác cao hơn
            download_link = response.urljoin(item.css('.item-shareholder__see a::attr(href)').get())

            if not title:
                continue

            iso_date = convert_date_to_iso8601(date_raw)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{title}\nDownload: {download_link}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        # Làm sạch chuỗi ngày (đôi khi có ký tự lạ hoặc khoảng trắng thừa)
        clean_date = vietnam_date_str.strip()
        date_object = datetime.strptime(clean_date, '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None