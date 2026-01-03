import scrapy
import sqlite3
import re
from stock_company_scraper.items import EventItem
from datetime import datetime, timedelta

class EventSpider(scrapy.Spider):
    name = 'event_vpx'
    mcpcty = 'VPX'
    allowed_domains = ['vpbanks.com.vn'] 
    start_urls = ['https://www.vpbanks.com.vn/quan-he-co-dong'] 

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

        # 2. Duyệt qua từng wrapper tin bài
        for post in response.css('div.item-link-wrapper'):
            title = post.css('[data-hook="post-title"] h2::text').get()
            url = post.css('a[data-hook="post-list-item__title"]::attr(href)').get()
            summary_text = post.css('[data-hook="post-description"] div.BOlnTh::text').get()
            publish_date_raw = post.css('[data-hook="time-ago"]::text').get()
            
            if not title:
                continue

            summary = title.strip()
            iso_date = convert_to_iso_date(publish_date_raw)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Wix post thường không có ID trong list, ta dùng summary + date làm khóa
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
            
            full_url = response.urljoin(url) if url else "N/A"
            desc = summary_text.strip() if summary_text else ""
            e_item['details_raw'] = f"{summary}\n{desc}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_to_iso_date(date_str):
    if not date_str:
        return None
    
    now = datetime.now()
    date_str = date_str.lower().strip()

    # 1. Xử lý "X ngày trước"
    if 'ngày trước' in date_str:
        days = int(re.search(r'\d+', date_str).group())
        return (now - timedelta(days=days)).strftime('%Y-%m-%d')

    # 2. Xử lý "giờ/phút trước"
    if 'giờ trước' in date_str or 'phút trước' in date_str:
        return now.strftime('%Y-%m-%d')

    # 3. Xử lý "DD thg MM" hoặc "DD/MM"
    match_date = re.search(r'(\d+)\s*(?:thg|/)\s*(\d+)', date_str)
    if match_date:
        day, month = int(match_date.group(1)), int(match_date.group(2))
        try:
            return datetime(now.year, month, day).strftime('%Y-%m-%d')
        except:
            return None

    return date_str