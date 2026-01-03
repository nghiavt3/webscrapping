import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_dxg'
    mcpcty = 'DXG'
    allowed_domains = ['ir.datxanh.vn'] 
    start_urls = ['https://ir.datxanh.vn/cong-bo-thong-tin'] 

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

        # 2. Nhắm mục tiêu vào tab "Công bố thông tin bất thường"
        container = response.css('#pills-unusual .filter-section-content')

        # Duyệt qua các khối năm (Đất Xanh thường hiển thị năm gần nhất lên đầu)
        for year_block in container.css('.year-element'):
            # Duyệt qua từng mục văn bản
            for item in year_block.css('.vanban-cbttbt.search-element'):
                title = item.css('a.search-query::text').get()
                detail_url = item.css('a.search-query::attr(href)').get()
                
                # Khối thông tin bổ sung chứa Ngày và Nút Tải
                date_download_block = item.css('.accordion-body')
                date_published = date_download_block.css('span::text').get()
                download_url = date_download_block.css('a[download]::attr(href)').get()
                
                if not title:
                    continue

                cleaned_title = title.strip()
                iso_date = convert_date_to_iso8601(date_published.strip())
                
                # Ưu tiên lấy download_url (PDF trực tiếp) nếu có
                final_link = response.urljoin(download_url if download_url else detail_url)

                # -------------------------------------------------------
                # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
                # -------------------------------------------------------
                event_id = f"{cleaned_title}_{iso_date}".replace(' ', '_').strip()[:150]
                
                cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
                if cursor.fetchone():
                    # Đất Xanh sắp xếp theo năm, nên nếu gặp tin cũ trong khối năm hiện tại thì có thể dừng
                    self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                    break 

                # 4. Yield Item
                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = cleaned_title
                e_item['date'] = iso_date
                e_item['details_raw'] = f"{cleaned_title}\nLink tài liệu: {final_link}"
                e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    input_format = "%d/%m/%Y"    
    output_format = '%Y-%m-%d'
    try:
        return datetime.strptime(vietnam_date_str.strip(), input_format).strftime(output_format)
    except Exception:
        return None