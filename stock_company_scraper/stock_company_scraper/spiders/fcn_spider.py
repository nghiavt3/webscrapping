import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_fcn'
    mcpcty = 'FCN'
    allowed_domains = ['fecon.com.vn'] 
    
    async def start(self):
        
        yield scrapy.Request(
                url='https://fecon.com.vn/cong-bo-thong-tin-c97', 
                callback=self.parse
                
            )
        
        yield scrapy.Request(
                url='https://fecon.com.vn/bao-cao-tai-chinh', 
                callback=self.parse_bctc,
                meta={'playwright': True}
            )
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

        # 2. Lấy danh sách bài viết
        articles = response.css('.item-news-home')

        for article in articles:
            # Trích xuất dữ liệu
            time_raw = article.css('.ct .control .time::text').get()
            title_raw = article.css('.ct .q-title a::text').get()
            url_raw = article.css('.ct .q-title a::attr(href)').get()

            if not title_raw:
                continue

            cleaned_title = title_raw.strip()
            cleaned_time = time_raw.strip() if time_raw else ""
            iso_date = convert_date_to_iso8601(cleaned_time)
            full_url = response.urljoin(url_raw)

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
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{cleaned_title}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

    def parse_bctc(self, response):
        # 1. Kết nối SQLite và chuẩn bị bảng
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

        # 2. Lấy danh sách bài viết
        rows = response.css('table#data-report tbody tr')

        for row in rows:
            # Trích xuất dữ liệu
            time_raw = row.css('td:nth-child(2)::text').get()
            title_raw = row.css('td.file-name::text').get()
            view_link = row.css('td:nth-child(3) a::attr(href)').get()
            url_raw = row.css('td:nth-child(4) a::attr(href)').get()

            if not title_raw:
                continue

            cleaned_title = title_raw.strip()
            cleaned_time = time_raw.strip() if time_raw else ""
            iso_date = convert_date_to_iso8601_2(cleaned_time)
            full_url = f"{response.urljoin(url_raw)}\n{response.urljoin(view_link)}"

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
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{cleaned_title}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    
    # Fecon sử dụng định dạng DD.MM.YYYY
    input_format = '%d.%m.%Y'
    output_format = '%Y-%m-%d'

    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        return date_object.strftime(output_format)
    except ValueError:
        return None
    
def convert_date_to_iso8601_2(vietnam_date_str):
    if not vietnam_date_str:
        return None
    
    # Fecon sử dụng định dạng DD.MM.YYYY
    input_format = '%d/%m/%Y'
    output_format = '%Y-%m-%d'

    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        return date_object.strftime(output_format)
    except ValueError:
        return None