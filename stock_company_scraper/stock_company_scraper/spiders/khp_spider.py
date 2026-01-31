import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_khp'
    mcpcty = 'KHP'
    allowed_domains = ['pckhanhhoa.cpc.vn'] 
    start_urls = [
        'https://pckhanhhoa.cpc.vn/vi-vn/quan-he-co-dong/thong-tin-bat-thuong',
        'https://pckhanhhoa.cpc.vn/vi-vn/quan-he-co-dong/thong-tin-dinh-ky',
        'https://pckhanhhoa.cpc.vn/vi-vn/quan-he-co-dong/thong-tin-co-dong'
        ] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def parse(self, response):
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

        for item in response.css('ul.news-main-list li.item'):
            pdf_link = item.css('h3.heading a::attr(href)').get()
            date_raw = item.css('p.description-note span:first-child::text').get(default='').strip()
            date_str = date_raw.split('-')[-1].strip()
            title_raw = item.css('h3.heading a::text').get()
            description= item.css('p.description::text').get()
            # Xử lý làm sạch chuỗi (loại bỏ khoảng trắng thừa ở đầu/cuối)
            if date_str:
                date_str = date_str.strip()
            if title_raw:
                title_raw = title_raw.strip()
            if not title_raw:
                continue

            summary = title_raw.strip()
            iso_date = convert_date_to_iso8601(date_str)
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
            e_item['details_raw'] = f"{description}\nLink: {full_pdf_url}"
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