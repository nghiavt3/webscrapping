import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_vck'
    mcpcty = 'VCK'
    allowed_domains = ['vps.com.vn'] 
    start_urls = ['https://vps.com.vn/ve-chung-toi/cong-bo-thong-tin'] 

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

        # 2. Duyệt qua từng card tin tức
        # Sử dụng selector linh hoạt cho CSS Modules
        cards = response.css('div[class*="styles_cardItem"]')
        
        for card in cards:
            title = card.css('div[class*="styles_title"]::text').get()
            link = card.css('a[class*="styles_btn_viewMore"]::attr(href)').get()
            raw_date = card.css('div[class*="styles_date"]::text').get()
            description = card.css('div[class*="styles_description"]::text').get()
            
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

            # 4. Đóng gói Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['date'] = iso_date
            
            full_url = response.urljoin(link) if link else "N/A"
            clean_desc = description.strip() if description else ""
            e_item['details_raw'] = f"{summary}\n{clean_desc}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        # Chuyển đổi "20/05/2024" -> "2024-05-20"
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None