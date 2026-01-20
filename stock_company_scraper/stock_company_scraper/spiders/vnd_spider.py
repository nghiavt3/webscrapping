import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_vnd'
    mcpcty = 'VND'
    allowed_domains = ['vndirect.com.vn'] 
    start_urls = ['https://www.vndirect.com.vn/danh_muc_quan_he_co_dong/cong-bo-thong-tin/'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
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

        items = response.css('div.news-item')

        for item in items:
            # 2. Tái cấu trúc ngày tháng từ các thành phần lẻ
            day_month = item.css('span.date-day::text').get() or ""
            month_part = item.css('sup::text').get() or "" 
            year = item.css('p.date-year::text').get() or ""
            
            # Làm sạch dấu "/" dư thừa nếu có trong month_part
            clean_month = month_part.replace(' ', '').strip()
            full_date = f"{day_month.strip()}{clean_month}/{year.strip()}"
            iso_date = convert_date_to_iso8601(full_date)

            # 3. Trích xuất tiêu đề
            title = (item.css('h3 a::text').get() or "").strip()

            # 4. Trích xuất danh sách file
            files = []
            file_elements = item.css('ul.listd li a')
            for f in file_elements:
                files.append({
                    'name': (f.css('::text').get() or "").strip(),
                    'url': response.urljoin(f.css('::attr(href)').get())
                })
            
            if not files:
                single_url = item.css('h3 a::attr(href)').get()
                if single_url and single_url.lower().endswith('.pdf'):
                    files.append({'name': title, 'url': response.urljoin(single_url)})

            if not title or not iso_date:
                continue

            # -------------------------------------------------------
            # 5. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT.")
                break 

            # 6. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{title}\nFiles: {str(files)}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    # Xử lý trường hợp chuỗi có thể là "20/ 12/2025" do ghép sup
    clean_date = vietnam_date_str.replace(' ', '')
    try:
        return datetime.strptime(clean_date, '%d/%m/%Y').strftime('%Y-%m-%d')
    except ValueError:
        return None