import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_dse'
    mcpcty = 'DSE'
    allowed_domains = ['ir.dnse.com.vn'] 
    start_urls = ['https://ir.dnse.com.vn/vi/ntag-cong-bo-thong-tin-16'] 

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

        # 2. Lặp qua từng thẻ .info-card
        for card in response.css('.info-card'):
            title = card.css('.info-title a::text').get()
            link = card.css('.info-title a::attr(href)').get()
            day = card.css('.highlight-day::text').get()
            month_year = card.css('.small-date::text').get() # VD: "12 - 2025"

            if not title:
                continue

            # Xử lý ngày tháng chuyên sâu cho DNSE
            iso_date = None
            if day and month_year:
                try:
                    # Làm sạch chuỗi: "12 - 2025" -> "12-2025"
                    my_clean = month_year.replace(' ', '').strip()
                    # Ghép thành "15/12-2025" để parse theo format %d/%m-%Y
                    raw_date_str = f"{day.strip()}/{my_clean}"
                    dt_obj = datetime.strptime(raw_date_str, '%d/%m-%Y')
                    iso_date = dt_obj.strftime('%Y-%m-%d')
                except Exception as e:
                    self.logger.error(f"Lỗi parse ngày DSE: {e}")

            cleaned_title = title.strip()
            full_url = response.urljoin(link) if link else ""

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{cleaned_title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item cho Pipeline
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{cleaned_title}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()