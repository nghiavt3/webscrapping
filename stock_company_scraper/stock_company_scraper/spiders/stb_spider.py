import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_stb'
    mcpcty = 'STB'
    allowed_domains = ['sacombank.com.vn'] 
    
    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        start_urls = [
        ('https://www.sacombank.com.vn/trang-chu/nha-dau-tu/cong-bo-thong-tin.html',self.parse ),
        ('https://www.sacombank.com.vn/trang-chu/nha-dau-tu/bao-cao.html', self.parse_bctc )
                  ] 

        for url ,callback in start_urls:
            yield scrapy.Request(
                url=url,
                callback=callback,
                # Kích hoạt Playwright để render nội dung động
                meta={"playwright": True}
            )

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

        # 2. Duyệt qua từng hàng trong bảng tin tức
        rows = response.css('table.table tbody tr.table__body-row')
        
        for row in rows:
            # Trích xuất dữ liệu từ các cột tương ứng
            title = row.css('.td--name p::text').get()
            file_url = row.css('.td--file a::attr(href)').get()
            publish_date = row.css('.td--date p::text').get()

            if not title or not publish_date:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso8601(publish_date.strip())
            full_file_url = response.urljoin(file_url)

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
            e_item['details_raw'] = f"{summary}\nFile: {full_file_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

    def parse_bctc(self, response):
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

        # 2. Duyệt qua từng hàng trong bảng tin tức
        report_rows = response.css('div.financial-report__body-row')
        
        for row in report_rows:
            # Trích xuất thông tin Quý (Header)
            period = row.css('h5.report-row__title::text').get()
            # Lặp qua từng item báo cáo trong khối đó
            items = row.css('div.report-row__item')
            for item in items:
                
                # Trích xuất dữ liệu từ các cột tương ứng
                title = item.css('p.report-row__item-text::text').get()
                file_url = item.css('a.report-row__item-pdf-btn[href*="/content/dam/"]::attr(href)').get()
                publish_date = None

            

                summary = title.strip()
                iso_date = publish_date
                full_file_url = response.urljoin(file_url)

                # -------------------------------------------------------
                # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
                # -------------------------------------------------------
                event_id = f"{summary}_{iso_date if iso_date else 'NODATE'}".replace(' ', '_').strip()[:150]
                
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
                e_item['details_raw'] = f"{summary}\nFile: {full_file_url}"
                e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                yield e_item

        conn.close()
def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        # Sacombank dùng định dạng DD/MM/YYYY
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None