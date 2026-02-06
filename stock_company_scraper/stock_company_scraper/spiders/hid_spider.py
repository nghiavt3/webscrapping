import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_hid'
    mcpcty = 'HID'
    allowed_domains = ['halcom.vn'] 
    
    async def start(self):
        year= datetime.now().year
        urls = [
            ('https://halcom.vn/category/quan-he-co-dong/cong-bo-thong-tin/', self.parse),  
            (f'https://halcom.vn/category/quan-he-co-dong/dai-hoi-dong-co-dong/{year}/', self.parse),     
            ('https://halcom.vn/bao-cao-quan-he-co-dong-2/', self.parse_bctc),  
        ]
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                callback=callback,
                #meta={'playwright': True}
            )
    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def parse(self, response):
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

        # 2. Container chính của Elementor Posts
        posts = response.css('article.elementor-post')
        
        for post in posts:
            # Trích xuất dữ liệu
            title = post.css('h3.elementor-post__title a::text').get()
            url = post.css('h3.elementor-post__title a::attr(href)').get()
            # Trích xuất ngày từ thẻ meta của Elementor (thường là span.elementor-post-date)
            date_raw = post.css('.elementor-post-date::text').get() or post.css('.elementor-post__meta-data span:first-child::text').get()

            if not title:
                continue

            cleaned_title = title.strip()
            # Halcom thường dùng định dạng d/m/Y hoặc H:M d/m/Y
            iso_date = convert_date_to_iso8601(date_raw.strip()) if date_raw else "NODATE"

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{cleaned_title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_title
            if iso_date == 'NODATE':
                e_item['date'] = None
            else :
                e_item['date'] = iso_date
            e_item['details_raw'] = f"{cleaned_title}\nLink: {url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

    async def parse_bctc(self, response):
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

        # 2. Container chính của Elementor Posts
        items = response.css('.elementor-accordion-item')
        
        for item in items:
            # Lấy tên năm (ví dụ: Năm tài chính 2023)
            year_text = item.css('.elementor-accordion-title::text').get()
        
            # Lặp qua danh sách các file trong năm đó
            file_list = item.css('.elementor-tab-content li')
            
            for file in file_list:
                # Lấy thẻ a chứa link và text
                link_node = file.css('a')
                
                title = link_node.css('::text').get()
                url = link_node.css('::attr(href)').get()
                
                if title and url:

                    cleaned_title = f"{year_text}-{title.strip()}"
                    summary= cleaned_title
                    # Halcom thường dùng định dạng d/m/Y hoặc H:M d/m/Y
                    iso_date = None

                    # -------------------------------------------------------
                    # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
                    # -------------------------------------------------------
                    event_id = f"{summary}_{iso_date if iso_date else 'NODATE'}".replace(' ', '_').strip()[:150]
                    
                    cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
                    if cursor.fetchone():
                        self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                        break 

                    # 4. Yield Item
                    e_item = EventItem()
                    e_item['mcp'] = self.mcpcty
                    e_item['web_source'] = self.allowed_domains[0]
                    e_item['summary'] = summary
                    e_item['date'] = iso_date
                    e_item['details_raw'] = f"{cleaned_title}\nLink: {url}"
                    e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    yield e_item

        conn.close()

def convert_date_to_iso8601(date_str):
    if not date_str:
        return None
    # Xử lý các biến thể ngày của Elementor
    date_str = date_str.strip()
    formats = ['%d/%m/%Y', '%H:%M %d/%m/%Y', '%d-%m-%Y']
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return None