import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_khg'
    mcpcty = 'KHG'
    allowed_domains = ['khaihoanland.vn'] 
    start_urls = ['https://khaihoanland.vn/quan-he-co-dong/'] 

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

        # 2. Chọn tất cả các mục thông báo (thẻ <a>) trong div.list-box
        report_items = response.css('div.list-box > a')

        for item in report_items:
            pdf_link = item.attrib.get('href')
            date_raw = item.css('.r-date::text').get()
            title_raw = item.css('.r-text p::text').get()
            # Xử lý làm sạch chuỗi (loại bỏ khoảng trắng thừa ở đầu/cuối)
            if date_raw:
                date_raw = date_raw.strip()
            if title_raw:
                title_raw = title_raw.strip()
            if not title_raw:
                continue

            summary = title_raw.strip()
            iso_date = convert_date_to_iso8601(date_raw)
            # Khải Hoàn Land thường dùng link tuyệt đối, nhưng urljoin vẫn an toàn hơn
            full_pdf_url = response.urljoin(pdf_link)

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
            e_item['details_raw'] = f"{summary}\nLink: {full_pdf_url}"
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