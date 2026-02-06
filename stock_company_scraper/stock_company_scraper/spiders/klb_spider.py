import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_klb'
    mcpcty = 'KLB'
    allowed_domains = ['kienlongbank.com'] 
    start_urls = ['https://kienlongbank.com/cong-bo-thong-tin',
                  'https://kienlongbank.com/bao-cao-tai-chinh',
                  'https://kienlongbank.com/dai-hoi-dong-co-dong'] 

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

        # 2. Lấy danh sách tất cả các thông báo
        items = response.css('div.list div.item')
        
        for item in items:
            title_raw = item.css('figcaption h5.title::text').get()
            link_raw = item.css('a::attr(href)').get()
            time_raw = item.css('figcaption span.time::text').get()

            if not title_raw:
                continue

            summary = title_raw.strip()
            # Xử lý URL tuyệt đối
            full_link = response.urljoin(link_raw.strip()) if link_raw else ""
            # Chuyển đổi ngày tháng
            iso_date = convert_date_to_iso8601(time_raw)

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
            e_item['details_raw'] = f"{summary}\nLink: {full_link}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None

    # Định dạng đầu vào của KienlongBank thường kèm theo giờ: 'DD/MM/YYYY - HH:MM'
    input_format = '%d/%m/%Y - %H:%M'
    output_format = '%Y-%m-%d'

    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        return date_object.strftime(output_format)
    except ValueError:
        # Thử fallback sang chỉ ngày nếu chuỗi không có giờ
        try:
            date_object = datetime.strptime(vietnam_date_str.strip()[:10], '%d/%m/%Y')
            return date_object.strftime(output_format)
        except:
            return None