import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_fpt'
    mcpcty = 'FPT'
    allowed_domains = ['fpt.com'] 
    start_urls = ['https://fpt.com/vi/nha-dau-tu/thong-tin-cong-bo'] 

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

        # 2. Lặp qua từng khối tháng
        months = response.css('div.media-download-section-key-information')
        
        for month_block in months:
            # Lặp qua từng tin tức bên trong khối tháng
            items = month_block.css('.media-download-section-key-information-content')
            
            for item in items:
                title = item.css('a.media-download-section-key-information-content-subtitle::text').get()
                link = item.css('a.media-download-section-key-information-content-subtitle::attr(href)').get()
                date_raw = item.css('.media-download-section-key-information-description-date::text').get()
                
                if not title:
                    continue

                # Làm sạch ngày (Xóa chữ "Cập nhật:")
                clean_date = date_raw.replace('Cập nhật:', '').strip() if date_raw else ""
                iso_date = convert_date_to_iso8601(clean_date)
                
                cleaned_title = title.strip()
                full_url = response.urljoin(link)

                # -------------------------------------------------------
                # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
                # -------------------------------------------------------
                event_id = f"{cleaned_title}_{iso_date}".replace(' ', '_').strip()[:150]
                
                cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
                if cursor.fetchone():
                    self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                    # Vì FPT sắp xếp tin mới nhất lên đầu, ta có thể dừng ngay lập tức
                    break 

                # 4. Yield Item
                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = cleaned_title
                e_item['date'] = iso_date
                e_item['details_raw'] = f"{cleaned_title}\nLink: {full_url}"
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