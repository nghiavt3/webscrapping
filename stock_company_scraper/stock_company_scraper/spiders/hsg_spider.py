import scrapy
import sqlite3
import json
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_hsg'
    mcpcty = 'HSG'
    allowed_domains = ['hoasengroup.vn'] 
    # Quét cả 2 trang: Thông báo và Công bố thông tin
    start_urls = [
        'https://hoasengroup.vn/vi/quan-he-co-dong/thong-bao-co-dong/26/',
        'https://hoasengroup.vn/vi/quan-he-co-dong/cong-bo-thong-tin/25/',
        'https://hoasengroup.vn/vi/quan-he-co-dong/bao-cao-tai-chinh/29/'
    ] 

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

        # 2. Duyệt qua từng mục thông báo
        for item in response.css('.info-item'):
            # Trích xuất dữ liệu cơ bản
            raw_date_text = item.css('.date::text').get()
            title_raw = item.css('.title-post::text').get()
            excerpt = item.css('.post-excerpt::text').get(default='').strip()
            
            # Xử lý ngày tháng: "Thứ Ba, 31/12/2025" -> "31/12/2025"
            date_part = raw_date_text.split(',')[-1].strip() if raw_date_text else None
            iso_date = convert_date_to_iso8601(date_part)

            # Trích xuất danh sách tài liệu đính kèm
            documents = []
            for doc in item.css('.item-sub'):
                doc_name = doc.css('.btn-viewer::text').get(default='').strip()
                view_url = response.urljoin(doc.css('.btn-viewer::attr(href)').get())
                if doc_name:
                    documents.append(f"{doc_name}: {view_url}")

            if not title_raw:
                continue

            title = title_raw.strip()

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
            
            # Gộp trích dẫn và danh sách link vào details
            doc_links = "\n".join(documents)
            e_item['details_raw'] = f"{title}\n{excerpt}\n{doc_links}"
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