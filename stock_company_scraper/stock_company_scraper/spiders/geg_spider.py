import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
from scrapy_playwright.page import PageMethod
import json
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
        url_bctc=[  
                    'https://web-be.geccom.vn/api/v2/front/post/tai-lieu-bao-cao/posts?search%5Bsession_tags.year_tags.id%3Ain%5D=15f911b7-7530-4626-be87-98156fe862db&search%5Bcategories.id%3Ain%5D=5d6a8d79-c9af-4223-bb46-edc58d3d3a22&page=1&limit=4',#2025
                    'https://web-be.geccom.vn/api/v2/front/post/tai-lieu-bao-cao/posts?search%5Bsession_tags.year_tags.id%3Ain%5D=3bf2555f-82d9-4e55-98d8-bf8c5fab87b0&search%5Bcategories.id%3Ain%5D=5d6a8d79-c9af-4223-bb46-edc58d3d3a22&page=1&limit=4',#2026
                    
                  ]
        for url in url_bctc:
            yield scrapy.Request(
                    url=url, 
                    callback=self.parse_json,
                    #meta={'playwright': True}
                )    
        
        # yield scrapy.Request(
        #     url= 'https://geccom.vn/quan-he-nha-dau-tu#baocao',
        #     meta={
        #         "playwright": True,
        #         "playwright_include_page": True,
        #         "playwright_page_methods": [
        #             # 1. Đợi và Click chọn Năm (ví dụ 2025)
        #             # 1. Đợi slide thật của năm 2025 xuất hiện (loại bỏ các bản sao cloned)
        #             PageMethod("wait_for_selector", ".slick-slide:not(.slick-cloned) >> text='2025'"),
                    
        #             # 2. Cuộn nó vào vùng nhìn thấy (quan trọng với slider dạng trượt)
        #             PageMethod("evaluate", "document.querySelector('.slick-slide:not(.slick-cloned) :text(\"2025\")').scrollIntoViewIfNeeded()"),
                    
        #             # 3. Click vào phần tử thật
        #             PageMethod("click", ".slick-slide:not(.slick-cloned) >> text='2025'"),
                    
        #             # 4. Chờ một chút để slider hoàn tất hiệu ứng trượt và load dữ liệu mới
        #             PageMethod("wait_for_timeout", 1000),
                    
        #             # 1. Đợi nút "Báo cáo Tài chính" xuất hiện
        #             PageMethod("wait_for_selector", "div.cursor-pointer >> p:has-text('Báo cáo Tài chính')"),
                    
        #             # 2. Click vào nút "Báo cáo Tài chính"
        #             # Sử dụng :has-text để Playwright tự tìm div cha chứa thẻ p đó và click
        #             PageMethod("click", "div.cursor-pointer:has(p:has-text('Báo cáo Tài chính'))"),
                    
        #             # 3. Đợi dữ liệu bảng hoặc danh sách file PDF tải xong
        #             # Bạn nên thay thế bằng selector của danh sách bài viết thực tế
        #             PageMethod("wait_for_load_state", "networkidle"), 
        #             PageMethod("wait_for_timeout", 1000),
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

    async def parse_json(self, response):
        # Chuyển đổi nội dung phản hồi thành JSON
        json_response = json.loads(response.text)
        
        # 1. Duyệt qua danh sách các bài viết trong 'data'
        posts = json_response.get('data', [])
        
        for post in posts:
            summary =post.get('title')
            iso_date = post.get('published_start')[:10]
            file_info = post.get('featured_image_1')
            link = file_info.get('path')
            
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{summary}\nLink: {link}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # item = {}
            
            # # Trích xuất thông tin văn bản
            # item['tieu_de'] = post.get('title')
            # item['slug'] = post.get('slug')
            # item['ngay_dang'] = post.get('published_start')
            
            # # Trích xuất thông tin file PDF từ 'featured_image_1'
            # file_info = post.get('featured_image_1')
            # if file_info:
            #     item['ten_file'] = file_info.get('title')
            #     item['link_file'] = file_info.get('path')
            #     item['alt_text'] = file_info.get('alt')
            # else:
            #     item['ten_file'] = None
            #     item['link_file'] = None
            
            # Trích xuất năm (từ session_tags)
            # year_tags = post.get('session_tags', {}).get('year_tags', [])
            # item['nam'] = year_tags[0].get('title') if year_tags else "N/A"
            
            yield e_item
    
def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None