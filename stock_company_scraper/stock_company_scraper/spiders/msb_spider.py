import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
from scrapy_playwright.page import PageMethod

class EventSpider(scrapy.Spider):
    name = 'event_msb'
    mcpcty = 'MSB'
    allowed_domains = ['msb.com.vn']
    current_year= datetime.now().year
    last_year =  current_year -1
    start_urls = [f'https://www.msb.com.vn/o/headless-delivery/v1.0/structured-content-folders/276690/structured-contents?filter=((contentStructureId%20eq%20169006)%20and%20(datePublished%20ge%20{last_year}-01-01T00:00:00Z%20and%20datePublished%20le%20{current_year}-12-31T23:59:59Z))&page=1&pageSize=7&sort=datePublished:desc',
                  #'https://www.msb.com.vn/o/headless-delivery/v1.0/structured-content-folders/276690/structured-contents?filter=((contentStructureId%20eq%20169006)%20and%20(datePublished%20ge%202026-01-01T00:00:00Z%20and%20datePublished%20le%202026-12-31T23:59:59Z))&page=1&pageSize=7&sort=datePublished:desc',

                  f'https://www.msb.com.vn/o/headless-delivery/v1.0/structured-content-folders/310641/structured-contents?filter=((contentStructureId%20eq%20169006)%20and%20(datePublished%20ge%20{last_year}-01-01T00:00:00Z%20and%20datePublished%20le%20{current_year}-12-31T23:59:59Z))&page=1&pageSize=7&sort=datePublished:desc',
                  #'https://www.msb.com.vn/o/headless-delivery/v1.0/structured-content-folders/310641/structured-contents?filter=((contentStructureId%20eq%20169006)%20and%20(datePublished%20ge%202025-01-01T00:00:00Z%20and%20datePublished%20le%202025-12-31T23:59:59Z))&page=1&pageSize=7&sort=datePublished:desc',

                  f'https://www.msb.com.vn/o/headless-delivery/v1.0/structured-content-folders/275880/structured-contents?filter=((contentStructureId%20eq%20169006)%20and%20(datePublished%20ge%20{last_year}-01-01T00:00:00Z%20and%20datePublished%20le%20{current_year}-12-31T23:59:59Z))&page=1&pageSize=7&sort=datePublished:desc',
                  #'https://www.msb.com.vn/o/headless-delivery/v1.0/structured-content-folders/275880/structured-contents?filter=((contentStructureId%20eq%20169006)%20and%20(datePublished%20ge%202025-01-01T00:00:00Z%20and%20datePublished%20le%202025-12-31T23:59:59Z))&page=1&pageSize=7&sort=datePublished:desc'
                  ] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def start(self):
        headers = {
            'Accept': 'application/xml', # Hoặc 'application/xml' nếu bạn chắc chắn nó là XML
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                headers=headers,
                meta={
                    "playwright": True,
                    "playwright_page_methods": [
                        # Chờ cho đến khi trang web vượt qua thử thách và hiện ra XML/JSON
                        PageMethod("wait_for_load_state", "networkidle"),
                        # Chờ thêm 2-3 giây để chắc chắn nội dung đã render xong
                        PageMethod("wait_for_timeout", 3000),
                    ],
                },
                callback=self.parse
            )

    async def parse(self, response):
        if "Challenge Validation" in response.text:
            self.logger.error("Vẫn bị kẹt tại trang Challenge. Cần tăng thời gian chờ.")
            return

        # Loại bỏ các namespace nếu kết quả trả về là XML để dễ query XPath
        response.selector.remove_namespaces()
        
        nodes = response.xpath('//items/items') # Theo cấu trúc file XML [cite: 2]
        
        if not nodes:
            self.logger.warning("Không tìm thấy dữ liệu. Có thể kết quả trả về là JSON.")
            # Bạn có thể thử parse JSON ở đây nếu cần
            return
        # 1. Kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        for item in response.xpath('//Page/items/items'):
            title = item.xpath('./title/text()').get()
            date_published = item.xpath('./datePublished/text()').get()
            relative_url = item.xpath('.//contentFieldValue/document/contentUrl/text()').get() 
            file_url = response.urljoin(relative_url) if relative_url else None
            if not title:
                continue
            
            summary = title.strip()
            iso_date = date_published[:10] if date_published else None
            
            
            url_absolute = file_url

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
            
            # cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            # if cursor.fetchone():
            #     self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT.")
            #     break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{summary}\nPDF: {url_absolute}"
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