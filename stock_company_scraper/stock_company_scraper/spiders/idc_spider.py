import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_idc'
    mcpcty = 'IDC'
    allowed_domains = ['admin.idico.com.vn'] 
    start_urls = ['https://admin.idico.com.vn/api/tai-lieus?populate=files.media&filters[category][$eq]=C%C3%B4ng%20b%E1%BB%91%20th%C3%B4ng%20tin&filters[files][title][$containsi]=&locale=vi'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                headers={
                    'Accept': 'application/json',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
            )

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

        # 2. Parse JSON an toàn
        try:
            data_list = response.json().get('data', [])
        except Exception as e:
            self.logger.error(f"Lỗi parse JSON: {e}")
            return

        for item in data_list:
            attributes = item.get('attributes', {})
            files = attributes.get('files', [])
            
            for file in files:
                title = file.get('title', '').strip()
                date_raw = file.get('override_date')
                # Truy cập sâu vào cấu trúc Strapi để lấy URL file
                media_attr = file.get('media', {}).get('data', {}).get('attributes', {})
                pdf_path = media_attr.get('url')
                
                if not title:
                    continue

                iso_date = convert_date_to_iso8601(date_raw)
                full_pdf_url = f"https://admin.idico.com.vn{pdf_path}" if pdf_path else ""

                # -------------------------------------------------------
                # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
                # -------------------------------------------------------
                event_id = f"{title}_{iso_date}".replace(' ', '_').strip()[:150]
                
                cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
                if cursor.fetchone():
                    self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT.")
                    # Vì Strapi API trả về list, tin cũ có thể nằm xen kẽ hoặc theo thứ tự, 
                    # ở đây ta dùng continue thay vì break nếu danh sách không đảm bảo thứ tự thời gian tuyệt đối
                    continue 

                # 4. Yield Item
                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = title
                e_item['date'] = iso_date
                e_item['details_raw'] = f"{title}\nLink: {full_pdf_url}"
                e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    # Xử lý định dạng ISO từ Strapi: "2025-12-31T07:08:46"
    input_format = '%Y-%m-%dT%H:%M:%S'
    output_format = '%Y-%m-%d'
    try:
        clean_date = vietnam_date_str.split('.')[0]
        date_object = datetime.strptime(clean_date, input_format)
        return date_object.strftime(output_format)
    except Exception:
        return None