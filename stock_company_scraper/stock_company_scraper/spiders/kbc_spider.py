import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_kbc'
    mcpcty = 'KBC'
    allowed_domains = ['kinhbaccity.vn'] 
    start_urls = ['https://kinhbaccity.vn/cong-bo-thong-tin.htm',
                  'https://kinhbaccity.vn/bao-cao-tai-chinh.htm',
                  'https://kinhbaccity.vn/dai-hoi-dong-co-dong.htm'] 

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

        # 2. Lấy danh sách các item tin tức
        items = response.css('div.dk-item')

        for item in items:
            date_raw = item.css('div.dk-item-date::text').get()
            title = item.css('h3.dk-item-title a::attr(title)').get()
            detail_url = item.css('h3.dk-item-title a::attr(href)').get()
            description = item.css('div.dk-item-desc::text').get()
            download_url = item.css('a.btndl-it::attr(href)').get()

            if not title:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso8601(date_raw)
            
            # Tạo link tuyệt đối cho chi tiết và file tải về
            abs_detail = response.urljoin(detail_url) if detail_url else ""
            abs_download = response.urljoin(download_url) if download_url else ""

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
            
            # Gộp mô tả ngắn và các loại link vào details_raw
            e_item['details_raw'] = f"Desc: {description}\nDetail: {abs_detail}\nDownload: {abs_download}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    input_format = '%d/%m/%Y'
    output_format = '%Y-%m-%d'
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        return date_object.strftime(output_format)
    except ValueError:
        return None