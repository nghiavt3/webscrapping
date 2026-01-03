import scrapy
import sqlite3
import re
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_vcg'
    mcpcty = 'VCG'
    allowed_domains = ['vinaconex.com.vn'] 
    start_urls = ['https://vinaconex.com.vn/quan-he-co-dong/thong-tin-chung?page=1'] 

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

        # 2. Định nghĩa Regex để tách ngày trong ngoặc đơn ở cuối chuỗi
        # Pattern này bắt cụm (DD/MM/YYYY) ở cuối
        date_pattern = re.compile(r'\s*\((?P<date>\d{2}/\d{2}/\d{4})\)$')

        records = response.css('div.list-info-generals__lists li')
        
        for record in records:
            full_text_raw = record.css('a::text').get()
            url_relative = record.css('a::attr(href)').get()
            
            if not full_text_raw:
                continue

            full_text = full_text_raw.strip()
            match = date_pattern.search(full_text)
            
            if match:
                date_string = match.group('date')
                # Dùng Regex .sub để xóa phần ngày tháng khỏi tiêu đề
                title = date_pattern.sub('', full_text).strip()
            else:
                date_string = None
                title = full_text

            iso_date = convert_date_to_iso8601(date_string)
            if not iso_date:
                iso_date = datetime.now().strftime('%Y-%m-%d')

            summary = title

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
            
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
            
            full_url = response.urljoin(url_relative) if url_relative else "N/A"
            e_item['details_raw'] = f"{summary}\nLink: {full_url}"
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