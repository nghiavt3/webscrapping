import scrapy
import sqlite3
import re
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_cat'
    mcpcty = 'CAT'
    allowed_domains = ['seaprimexco.com'] 
    start_urls = ['https://seaprimexco.com/vi/shareholder_relation'] 

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

        # 2. Định vị danh sách tin tức
        news_selectors = response.css('div.news-item-list')
        
        if not news_selectors:
            self.logger.warning("Không tìm thấy dữ liệu với class 'div.news-item-list'")
            return

        for selector in news_selectors:
            title = selector.css('h3::text').get()
            date_raw = selector.css('p strong::text').get()
            download_url = selector.css('div.btn-haisan a::attr(href)').get()

            if not title or not download_url:
                continue

            # Làm sạch dữ liệu
            clean_title = title.strip()
            iso_date = convert_vietnamese_date(date_raw)
            full_pdf_url = response.urljoin(download_url)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Tạo ID duy nhất (Tiêu đề + Ngày)
            event_id = f"{clean_title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{clean_title}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item nếu là tin mới
            item = EventItem()
            item['mcp'] = self.mcpcty
            item['web_source'] = self.allowed_domains[0]
            item['summary'] = clean_title
            item['date'] = iso_date
            item['details_raw'] = f"{clean_title}\nLink PDF: {full_pdf_url}"
            item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield item

        conn.close()

def convert_vietnamese_date(date_str):
    """
    Xử lý định dạng: '28 Th11 2025' -> '2025-11-28'
    """
    if not date_str:
        return None

    # Chuẩn hóa khoảng trắng và viết thường
    date_str = re.sub(r'\s+', ' ', date_str.strip().lower())
    
    # Map viết tắt tháng của Seaprimexco
    month_mapping = {
        'th10': '10', 'th11': '11', 'th12': '12',
        'th1': '01', 'th2': '02', 'th3': '03', 'th4': '04', 
        'th5': '05', 'th6': '06', 'th7': '07', 'th8': '08', 'th9': '09'
    }
    
    for vn_month, num_month in month_mapping.items():
        if vn_month in date_str:
            date_str = date_str.replace(vn_month, num_month)
            break
    
    try:
        # Kỳ vọng định dạng: "28 11 2025"
        date_object = datetime.strptime(date_str, "%d %m %Y")
        return date_object.strftime("%Y-%m-%d")
    except ValueError:
        return None