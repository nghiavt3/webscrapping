import scrapy
import sqlite3
import os
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_bqp'
    mcpcty = 'BQP'
    allowed_domains = ['bqp.com.vn'] 
    start_urls = ['https://bqp.com.vn/quan-he-co-dong/'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # 1. Kết nối SQLite
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

        # 2. Chọn tất cả các khối lớn chứa danh mục (productst-e)
        category_blocks = response.css('div.productst-e')

        for block in category_blocks:
            # Chọn tất cả các mục tài liệu (sukien-item)
            items = block.css('div.sukien-item')
            
            for item in items:
                # 3. Trích xuất và ghép nối Ngày tháng
                # BQP tách: <span class="date-new">29</span> và <span class="month-new">12/2025</span>
                date_part = item.css('span.date-new::text').get()
                month_year_part = item.css('span.month-new::text').get()
                
                full_date_raw = f"{date_part.strip()}/{month_year_part.strip()}" if date_part and month_year_part else ""
                iso_date = convert_date_to_iso8601(full_date_raw)

                # 4. Trích xuất Tiêu đề và Link
                title = item.css('div.sukien-title a::text').get()
                title = title.strip() if title else ""
                
                link = response.urljoin(item.css('div.sukien-title a::attr(href)').get())
                download_link = response.urljoin(item.css('div.sharaholder-us-down a[title="Tải tài liệu"]::attr(href)').get())

                # -------------------------------------------------------
                # 5. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
                # -------------------------------------------------------
                event_id = f"{title}_{iso_date}".replace('/', '-').replace('.', '_').strip()[:150]
                
                cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
                if cursor.fetchone():
                    self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG NHÁNH QUÉT NÀY.")
                    break # Dừng quét nhánh danh mục này

                # 6. Đóng gói Item
                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = title
                e_item['details_raw'] = f"{title}\nXem: {link}\nTải: {download_link}"
                e_item['date'] = iso_date 
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