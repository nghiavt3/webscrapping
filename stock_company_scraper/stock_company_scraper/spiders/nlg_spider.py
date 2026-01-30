import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_nlg'
    mcpcty = 'NLG'
    allowed_domains = ['namlongvn.com'] 
    start_urls = ['https://www.namlongvn.com/quan-he-nha-dau-tu/'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
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

        # 2. Chọn các khối tài liệu trong section Công bố thông tin
        doc_items = response.css('#info-disclosure-section .doc-item')

        for item in doc_items:
            title_node = item.css('.doc-title a::text').get()
            link = item.css('.doc-title a::attr(href)').get()
            datetime_label = item.css('.doc-meta .datetime-label::text').get()

            if not title_node:
                continue

            summary = title_node.strip()
            # Chuyển đổi ngày (Hàm hỗ trợ định dạng "DD/MM/YYYY | HH:MM")
            iso_date = convert_date_to_iso8601(datetime_label)
            full_url = response.urljoin(link)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
            
            # cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            # if cursor.fetchone():
            #     self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT.")
            #     break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{summary}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        # Xử lý định dạng đặc thù của Nam Long: "DD/MM/YYYY | HH:MM"
        input_format = "%d/%m/%Y | %H:%M"
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        # Backup nếu định dạng chỉ có ngày
        try:
            date_object = datetime.strptime(vietnam_date_str.strip()[:10], "%d/%m/%Y")
            return date_object.strftime('%Y-%m-%d')
        except:
            return None