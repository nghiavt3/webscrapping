import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_hbc'
    mcpcty = 'HBC'
    allowed_domains = ['hbcg.vn'] 
    start_urls = ['https://hbcg.vn/report/news.html'] 

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

        # 2. Lấy danh sách các khối tin tức
        news_items = response.css('div.gridBlock-content a')
    
        for item in news_items:
            # Trích xuất dữ liệu
            title = item.css('p.txt7::text').get()
            summary_desc = item.css('p.gridBlock-description::text').get()
            date_raw = item.css('p.date-info::text').get()
            pdf_url = item.css('::attr(href)').get()

            if not title:
                continue

            # Làm sạch dữ liệu
            cleaned_title = title.strip()
            # Xử lý chuỗi "Cập nhật ngày: DD/MM/YYYY"
            date_clean = date_raw.replace('Cập nhật ngày:', '').strip() if date_raw else ""
            iso_date = convert_date_to_iso8601(date_clean)
            full_pdf_url = response.urljoin(pdf_url)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Tạo ID duy nhất dựa trên tiêu đề và ngày
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
            e_item['details_raw'] = f"{cleaned_title}\n{summary_desc.strip() if summary_desc else ''}\nLink: {full_pdf_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None