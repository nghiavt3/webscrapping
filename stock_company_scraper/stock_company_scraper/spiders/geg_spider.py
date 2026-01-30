import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
from scrapy_playwright.page import PageMethod
class EventSpider(scrapy.Spider):
    name = 'event_geg'
    mcpcty = 'GEG'
    allowed_domains = ['geccom.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def start(self):
        urls = [
            ('https://geccom.vn/cong-bo-thong-tin/bat-thuong', self.parse_generic),
            ('https://geccom.vn/dai-hoi-dong-co-dong/dhdcd-thuong-nien', self.parse_generic),   
           ('https://geccom.vn/cong-bo-thong-tin/thong-bao', self.parse_generic),
              
        ]
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                callback=callback,
                meta={'playwright': True}
            )
        
        # yield scrapy.Request(
        #     url= 'https://geccom.vn/quan-he-nha-dau-tu#baocao',
        #     meta={
        #         "playwright": True,
        #         "playwright_include_page": True,
        #         "playwright_page_methods": [
        #             # 1. Đợi và Click chọn Năm (ví dụ 2025)
        #             PageMethod("wait_for_selector", "//p[text()='2025']"),
        #             PageMethod("click", "//p[text()='2025']"),
                    
        #             # 2. Nghỉ một chút để DOM cập nhật danh sách các loại báo cáo
        #             PageMethod("wait_for_timeout", 1000), 
                    
        #             # 3. Đợi và Click nút "Báo cáo Tài chính"
        #             # Lưu ý: Viết đúng hoa thường như trong HTML: "Báo cáo Tài chính"
        #             PageMethod("wait_for_selector", "//div[p[text()='Báo cáo Tài chính']]"),
        #             PageMethod("click", "//div[p[text()='Báo cáo Tài chính']]"),
                    
        #             # 4. Đợi cho đến khi bảng dữ liệu thực sự hiện ra
        #             # Thay '.content-table' bằng selector vùng chứa file PDF của trang GEC
        #             PageMethod("wait_for_selector", ".flex-col", timeout=10000), 
        #         ],
        #     },
        #     callback=self.parse_bctc
        # )


    def parse_generic(self, response):
        """Hàm parse dùng chung cho các chuyên mục của SeABank"""
        # 1. Khởi tạo SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
       # cursor.execute(f'''DROP TABLE IF EXISTS {table_name}''')
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # Chọn tất cả các hàng dữ liệu
        items = response.css(r'div.flex.flex-col.gap-2.text-\[14px\]')

        for item in items:
            title = item.css('a p::text').get()
            relative_url = item.css('a::attr(href)').get()
            date_str = item.css('div.flex.gap-2.items-center p::text').get()

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
            e_item['details_raw'] = f"{summary}\nLink: {absolute_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

    async def parse_bctc(self, response):
        # Lưu file để kiểm tra giao diện
        with open("debug_page.html", "wb") as f:
            f.write(response.body)
    
        self.logger.info("Đã lưu trang vào file debug_page.html. Hãy mở nó để kiểm tra!")
        """Hàm parse dùng chung cho các chuyên mục của SeABank"""
        # 1. Khởi tạo SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
       # cursor.execute(f'''DROP TABLE IF EXISTS {table_name}''')
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # Chọn tất cả các hàng dữ liệu
        report_items = response.css('div.flex.flex-col.gap-2')

        for item in report_items:
            title = item.css('p.text-\\[\\#101828\\].font-semibold::text').get()
            relative_url = item.css('a.flex::attr(href)').get()
            date_str = item.css('div.flex.gap-2.items-center p::text').get()

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
            e_item['details_raw'] = f"{summary}\nLink: {absolute_url}"
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