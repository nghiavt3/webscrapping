import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
from scrapy_playwright.page import PageMethod
class EventSpider(scrapy.Spider):
    name = 'event_plx'
    mcpcty = 'PLX'
    allowed_domains = ['petrolimex.com.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def start(self):
        urls = [
            ('https://www.petrolimex.com.vn/ndi/thong_tin_co_dong.html', self.parse_ttcd),
           # ('https://www.petrolimex.com.vn/bao-cao-nha-dau-tu.html#type-tab-7', self.parse_bcndt),
            
        ]
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                callback=callback,
                #meta={ "playwright": True,}
            )

    def parse_ttcd(self, response):
        """Hàm parse dùng chung cho các chuyên mục của SeABank"""
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

        # Chọn tất cả các hàng dữ liệu
        # Chọn tất cả các khối tin bài
        posts = response.css('.post-default')

        for post in posts:
            meta_text = post.css('.post-default__meta::text').getall()
            # meta_text thường trả về [' | ', '28/11/2025']
            date_str = meta_text[-1].replace('|', '').strip() if meta_text else None

            title = post.css('.post-default__title a::text').get()
            relative_url = post.css('.post-default__title a::attr(href)').get()
            

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

    def parse_bcndt(self, response):
            """Hàm parse dùng chung cho các chuyên mục của SeABank"""
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

            # 1. Lấy danh sách tên các Tab từ menu điều hướng (BCTC, BCTN, ...)
            # Tạo mapping giữa ID của tab và tên hiển thị để dữ liệu rõ ràng hơn
            tab_mapping = {}
            nav_items = response.css('ul#myTab li.nav-item a')
            for nav in nav_items:
                target_id = nav.attrib.get('href', '').replace('#', '') 
                tab_name = nav.css('::text').get()
                if target_id and tab_name:
                    tab_mapping[target_id] = tab_name.strip()

            # 2. Duyệt qua tất cả các nội dung tab (tab-pane) hiện có trong HTML
            panes = response.css('div.tab-content div.tab-pane.active.show')
            
            for pane in panes:
                pane_id = pane.attrib.get('id')
                category = tab_mapping.get(pane_id, "Không xác định")

                # 3. Trích xuất từng tài liệu trong danh sách của tab đó
                items = pane.css('ul.document-list li.item')
                for item in items:
                    title = item.css('h3.item__title a::text').get()
                    link = item.css('h3.item__title a::attr(href)').get()
                    
                    # Xử lý ngày tháng: thường nằm trong span cuối cùng của item__meta
                    date = item.css('.item__meta span:last-child::text').get()
                    

                    if not title or not date:
                        continue

                    summary = title.strip()
                    iso_date = convert_date_to_iso8601(date)
                    absolute_url = response.urljoin(link)

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