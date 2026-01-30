import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_nvb'
    mcpcty = 'NVB'
    allowed_domains = ['ncb-bank.vn'] 
    start_urls = ['https://www.ncb-bank.vn/vi/nha-dau-tu',
                  'https://www.ncb-bank.vn/vi/nha-dau-tu/bao-cao-tai-chinh',
                  'https://www.ncb-bank.vn/vi/nha-dau-tu/nghi-quyet-dhdcd-va-hdqt'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def start(self):
        url = self.start_urls[0]
        yield scrapy.Request(
            url,
            meta={
                "playwright": True,
                "playwright_include_page": True,
            },
            callback=self.parse
        )
    def parse(self, response):
        # 1. Kết nối và khởi tạo SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Lấy danh sách các bài viết (Elementor Custom Skin Loop)
        # Lấy tất cả các khối tin
        # Tìm tất cả các tiêu đề bản tin
        titles_h6 = response.css('h6.new-download')
        
        for h6 in titles_h6:
            # 1. Lấy Title
            title = "".join(h6.xpath('./text()').getall()).strip()
            if not title:
                title = h6.css('::text').get('').strip()

            # 2. Lấy Date
            # Cách 1: Tìm p bên trong h6
            date_text = h6.css('p::text').get()
            # Cách 2: Nếu không thấy, tìm thẻ p là anh em ngay phía sau (nếu DOM bị vỡ)
            if not date_text:
                date_text = h6.xpath('./following-sibling::p[1]/text()').get()
            
            # Làm sạch date
            clean_date = "N/A"
            if date_text:
                import re
                # Dùng Regex để tìm định dạng dd/mm/yyyy trong chuỗi
                match = re.search(r'\d{2}/\d{2}/\d{4}', date_text)
                if match:
                    date_part = match.group()
                    try:
                        clean_date = datetime.strptime(date_part, "%d/%m/%Y").strftime("%Y-%m-%d")
                    except:
                        clean_date = date_part

            # 3. Lấy File URL
            # Cách 1: Tìm a bên trong h6
            file_url = h6.css('a::attr(href)').get()
            # Cách 2: Tìm a là anh em ngay phía sau (nếu DOM bị vỡ)
            if not file_url:
                file_url = h6.xpath('./following-sibling::a[1]/@href').get()
            
            if file_url:
                file_url = response.urljoin(file_url.strip())

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{title}_{clean_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['date'] = clean_date
            e_item['details_raw'] = f"Title: {title}\nPDF: {file_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()