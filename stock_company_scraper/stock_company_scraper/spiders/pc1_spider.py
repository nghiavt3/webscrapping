import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
from scrapy_playwright.page import PageMethod
class EventSpider(scrapy.Spider):
    name = 'event_pc1'
    mcpcty = 'PC1'
    allowed_domains = ['pc1group.vn'] 
    #start_urls = ['https://www.pc1group.vn/category/quan-he-dau-tu/cong-bo-thong-tin/'] 
    def start_requests(self):
        # Chúng ta truy cập trang chủ trước để Cloudflare "duyệt" trình duyệt
        url = "https://www.pc1group.vn/category/quan-he-dau-tu/cong-bo-thong-tin/"
        
        yield scrapy.Request(
            url,
            meta={
                "playwright": True,
                "playwright_include_page": True, # Giữ trang để xử lý nếu cần
                "playwright_page_methods": [
                    # 1. Chờ lâu hơn (60 giây) thay vì 30 giây mặc định
                    PageMethod("wait_for_selector", ".vc_grid-item", timeout=60000),
                    # 2. Thêm thao tác cuộn chuột để kích hoạt load dữ liệu
                    PageMethod("evaluate", "window.scrollBy(0, 500)"),
                ],
            },
            # Sử dụng User-Agent giống hệt trình duyệt bạn đang dùng
            headers={
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0'
            },
            callback=self.parse
        )

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

        # 2. Lấy danh sách các item (Visual Composer Grid)
        items = response.css('.vc_grid-item.vc_clearfix.vc_col-sm-6.vc_visible-item')

        for item in items:
            title = item.css('h4 a::text').get()
            url = item.css('h4 a::attr(href)').get()
            
            # Selector ngày tháng đặc thù của Visual Composer
            date_raw = item.css('.vc_gitem-post-data-source-post_date div::text').get()
            
            if not title:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso8601(date_raw)
            full_url = response.urljoin(url)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Dùng summary và date để tạo ID duy nhất
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
            e_item['details_raw'] = f"{summary}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        # Xử lý trường hợp ngày tháng có khoảng trắng thừa
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None