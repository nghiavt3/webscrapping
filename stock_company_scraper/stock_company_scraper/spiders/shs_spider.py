import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_shs'
    mcpcty = 'SHS'
    allowed_domains = ['archive.shs.com.vn'] 
    start_urls = ['https://archive.shs.com.vn/ShareHolder.aspx'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # 1. Khởi tạo kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Xử lý các tin nổi bật (div.textnews)
        for head_news in response.css('div.textnews'):
            title = head_news.css('a::text').get(default='').strip()
            url = response.urljoin(head_news.css('a::attr(href)').get())
            publish_date = head_news.css('span.timestamp::text').get(default='').strip()

            if title:
                yield from self.process_item(cursor, table_name, title, publish_date, url)

        # 3. Xử lý danh sách tin tức qua timestamps
        timestamps = response.css('span.timestamp')
        for ts in timestamps:
            raw_date = ts.css('::text').get()
            # Tìm thẻ <a> chứa link 'News' trong phạm vi cha
            row = ts.xpath('..')
            title_node = row.xpath('.//a[contains(@href, "News")]')
            title_text = title_node.xpath('string(.)').get()
            link = title_node.xpath('./@href').get()

            if title_text and "Tin tức" not in title_text:
                full_url = response.urljoin(link)
                yield from self.process_item(cursor, table_name, title_text.strip(), raw_date, full_url)

        conn.close()

    def process_item(self, cursor, table_name, title, raw_date, url):
        """Hàm bổ trợ để kiểm tra trùng lặp và yield item."""
        iso_date = convert_date_to_iso8601(raw_date)
        event_id = f"{title}_{iso_date}".replace(' ', '_').strip()[:150]
        
        cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
        if not cursor.fetchone():
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{title}\nLink: {url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            yield e_item

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    # Chuyển đổi định dạng thời gian Việt Nam sang chuẩn quốc tế để parse
    date_str = vietnam_date_str.replace('SA', 'AM').replace('CH', 'PM').strip()
    try:
        # SHS dùng định dạng: 31/12/2025 08:30 AM
        date_object = datetime.strptime(date_str, '%d/%m/%Y %I:%M %p')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        # Fallback nếu không có giờ phút
        try:
            date_object = datetime.strptime(date_str.split()[0], '%d/%m/%Y')
            return date_object.strftime('%Y-%m-%d')
        except:
            return None