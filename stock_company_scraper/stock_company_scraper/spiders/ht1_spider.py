import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import json
from scrapy.selector import Selector
class EventSpider(scrapy.Spider):
    name = 'event_ht1'
    mcpcty = 'HT1'
    allowed_domains = ['vicemhatien.com.vn'] 
    start_urls = ['https://www.vicemhatien.com.vn/api/shareholder-documents?language=vi&limit=100&offset=0']
    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def parse(self, response):
        """Hàm parse dùng chung cho các chuyên mục của SeABank"""
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

        # Chọn bảng có class 'tablecodong' và lấy tất cả các dòng <tr>
        # Sử dụng :not(:first-child) để bỏ qua dòng tiêu đề (Văn bản, Ngày ban hành...)
        data = json.loads(response.text)
        # Giả sử cấu trúc JSON trả về có danh sách tài liệu trong 'data' hoặc 'items'
        # Bạn có thể kiểm tra cấu trúc chính xác bằng cách mở link API trên trình duyệt
        items = data.get('documents', [])

        for item in items:
            title = item.get('title', '')
            # 1. Lấy chuỗi HTML từ trường content
            content_html = item.get('content', '')
            # 2. Sử dụng Selector để tìm tất cả các thẻ <a> và lấy href
            # Dùng .getall() vì một số bài (như BCTC) có nhiều hơn 1 file PDF
            links = Selector(text=content_html).css('a::attr(href)').getall()
            
            # 3. Chuẩn hóa link (vì link trong JSON là relative dạng "../uploads/...")
            full_links = [response.urljoin(link) for link in links]
            #relative_url = row.css('td:nth-child(1) a::attr(href)').get()
            date_str = item.get('publishDate', '')

            if not title or not date_str:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso8601(date_str)
            #absolute_url = response.urljoin(relative_url)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT CHUYÊN MỤC.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{summary}\nLink: {full_links}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), '%Y-%m-%d %H:%M:%S')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None