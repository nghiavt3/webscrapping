import scrapy
import sqlite3
import re
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_pnj'
    mcpcty = 'PNJ'
    allowed_domains = ['pnj.com.vn'] 
    start_urls = [
        'https://www.pnj.com.vn/quan-he-co-dong/thong-bao/',
        'https://www.pnj.com.vn/quan-he-co-dong/bao-cao-tai-chinh/'
                  ] 

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

        # 2. Xử lý HTML thô từ container 'div.answer'
        container = response.css('div.answer')
        raw_html = container.get()
        if not raw_html:
            return

        # Tách các dòng bằng thẻ <br>
        rows = re.split(r'<br\s*/?>', raw_html)

        for row in rows:
            sel = scrapy.Selector(text=row)
            full_text = "".join(sel.css('::text').getall()).strip()
            
            # Chỉ xử lý các dòng có dấu + (định dạng thông báo của PNJ)
            # if not full_text or '+' not in full_text:
            #     continue

            # Trích xuất ngày tháng bằng Regex
            date_match = re.search(r'(\d{2}/\d{2}/\d{4})', full_text)
            date_raw = date_match.group(1) if date_match else ""
            iso_date = convert_date_to_iso8601(date_raw) if date_raw else "1970-01-01"

            # Trích xuất danh sách File đính kèm
            links = []
            for a in sel.css('a'):
                links.append({
                    'label': a.css('::text').get(default='Download').strip(),
                    'url': response.urljoin(a.css('::attr(href)').get())
                })

            # Làm sạch tiêu đề
            clean_title = full_text.split('):')[0].replace('+', '').strip()
            if '(' not in clean_title and date_raw:
                clean_title += f" ({date_raw})"

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{clean_title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{clean_title}]. BỎ QUA.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = clean_title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"Title: {clean_title}\nLinks: {str(links)}"
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