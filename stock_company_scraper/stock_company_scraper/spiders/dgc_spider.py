import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_dgc'
    mcpcty = 'DGC'
    allowed_domains = ['ducgiangchem.vn'] 
    start_urls = ['https://ducgiangchem.vn/category/quan-he-co-dong/thong-bao/',
                  'https://ducgiangchem.vn/category/quan-he-co-dong/bao-cao-tai-chinh/',
                  'https://ducgiangchem.vn/category/quan-he-co-dong/dai-hoi-co-dong/'] 

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
                id TEXT PRIMARY KEY,
                mcp TEXT,
                date TEXT,
                summary TEXT,
                scraped_at TEXT,
                web_source TEXT,
                details_clean TEXT
            )
        ''')

        # 2. Lặp qua tất cả các bài viết
        for article in response.css('article.post'):
            # Trích xuất Ngày và Tháng/Năm
            day = article.css('p.meta span.day::text').get()
            month_year = article.css('p.meta span.month::text').get()
            
            day_clean = day.strip() if day else ''
            month_year_clean = month_year.strip() if month_year else ''
            
            # Trích xuất dữ liệu khác
            title_raw = article.css('h2.title-post a::text').get()
            url = article.css('h2.title-post a::attr(href)').get()
            summary = article.css('div.entry-post p::text').get()
            
            if not title_raw:
                continue

            cleaned_title = title_raw.strip()
            iso_date = convert_date_to_iso(day_clean, month_year_clean)
            full_url = response.urljoin(url)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Tạo ID duy nhất (Tiêu đề + Ngày)
            event_id = f"{cleaned_title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{cleaned_title}\n{summary.strip() if summary else ''}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso(day_raw, month_year_raw):
    """Chuyển đổi từ định dạng '11' và '2025, Nov' sang '2025-11-11'."""
    if not day_raw or not month_year_raw:
        return None
    
    # Làm sạch chuỗi: '2025, Nov' -> '2025 Nov'
    month_year = month_year_raw.strip().replace(',', ' ')
    full_date_string = f"{day_raw.strip()} {month_year}"
    
    # Định dạng: %d (ngày), %Y (năm), %b (tháng viết tắt Tiếng Anh như Nov, Dec...)
    input_format = "%d %Y %b"
    
    try:
        date_object = datetime.strptime(full_date_string, input_format)
        return date_object.strftime("%Y-%m-%d")
    except ValueError:
        return None