import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_btp'
    mcpcty = 'BTP'
    allowed_domains = ['btp.com.vn'] 
    start_urls = ['https://www.btp.com.vn/c2/vi-VN/bao-chi-2/Quan-he-co-dong-6'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def parse(self, response):
        # 1. Khởi tạo SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        #cursor.execute(f'''DROP TABLE IF EXISTS {table_name}''')
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')
        for doc in response.css('ul.npt-document-list li'):
            date_raw = doc.css('.time-day::text').get()
            title_raw = doc.css('.title-document h6::text').get()
            url = response.urljoin(doc.css('a.title-document::attr(href)').get())
        
            if not date_raw or not title_raw:
                continue

            summary = title_raw.strip()
            iso_date = convert_date_to_iso8601(date_raw.strip())
            
            
            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{iso_date if iso_date else 'NODATE'}".replace(' ', '_').strip()[:150]
            
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

            e_item['details_raw'] = f"Link:\n{url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item
        conn.close()
    def parse_detail(self, response):
        # Nhận lại item từ trang danh sách gửi qua meta
        item = response.meta['item']
        
        # 1. Trích xuất tất cả các link PDF trong khối chi tiết
        # Chúng ta tìm các thẻ <a> nằm trong .blog-details-col có href chứa ".pdf"
        pdf_elements = response.css('.blog-details-col a[href$=".pdf"]')
        
        files = []
        for anchor in pdf_elements:
            file_name = anchor.css('::text').get()
            file_url = anchor.css('::attr(href)').get()
            
            if file_url:
                files.append({
                    'file_name': file_name.strip() if file_name else "Untitled",
                    'file_url': response.urljoin(file_url) # Đảm bảo link tuyệt đối
                })
        
        # 2. Gán danh sách file vào item
        item['details_raw'] = f"{item['details_raw']}/n pdf:{files}"
        
        # 3. Trả về item hoàn chỉnh cho Pipeline
        yield item
        
def convert_date_to_iso8601(vietnam_date_str):
    """Xử lý định dạng DD.MM.YYYY của BVH"""
    if not vietnam_date_str:
        return None
    try:
        # BVH dùng dấu chấm nên parse trực tiếp theo định dạng này
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return vietnam_date_str