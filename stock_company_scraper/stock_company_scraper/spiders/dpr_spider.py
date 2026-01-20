import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_dpr'
    mcpcty = 'DPR'
    allowed_domains = ['doruco.com.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        urls = [
            ('https://www.doruco.com.vn/quan-he-co-dong/thong-tin-co-dong-vn-b-86-0.html', self.parse_generic),
            ('https://www.doruco.com.vn/quan-he-co-dong/bao-cao-tai-chinh-vn-b-87-0.html', self.parse_generic),
             ('https://www.doruco.com.vn/quan-he-co-dong/bao-cao-thuong-nien-vn-b-88-0.html', self.parse_generic),
             #('http://www.thaiduongpetrol.vn/ban-tin-nha-dau-tu', self.parse_generic),
             
            
        ]
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                callback=callback,
                #meta={'playwright': True}
            )

    def parse_generic(self, response):
        """Hàm parse dùng chung cho các chuyên mục của SeABank"""
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

        # Chọn tất cả các hàng dữ liệu
        items = response.css('.bc-list, .bc-list-1')

        for item in items:
            date_str = item.css('.news-date::text').get()
            # 2. Trích xuất tiêu đề 
            # Lấy toàn bộ text trong thẻ a đầu tiên, sau đó loại bỏ phần text của ngày tháng
            full_a_text = item.xpath('./a[1]//text()').getall()
            # Ghép lại và loại bỏ phần ngày tháng để lấy tiêu đề sạch
            title = "".join([t for t in full_a_text if t not in (date_str or "")]).strip()
            relative_url = item.css('.bc-list-down-in a[href*="upload_file"]::attr(href)').get()
            post_link = response.urljoin(item.css('a::attr(href)').get())
           

            if not title or not date_str:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso8601(date_str)
            absolute_url = response.urljoin(relative_url)

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
            e_item['details_raw'] = f"{summary}\nLink: {absolute_url} \n {post_link}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%y, %I:%M %p')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None