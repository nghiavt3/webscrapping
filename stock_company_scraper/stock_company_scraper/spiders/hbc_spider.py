import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
from scrapy_playwright.page import PageMethod
class EventSpider(scrapy.Spider):
    name = 'event_hbc'
    mcpcty = 'HBC'
    allowed_domains = ['hbcg.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def start(self):
        
        year = datetime.now().year-1
        yield scrapy.Request(
                url='https://hbcg.vn/report/news.html', 
                callback=self.parse_bctc,
            )
        yield scrapy.Request(
            url = "https://hbcg.vn/report/financial.html",
            callback=self.parse_bctc
        )
        yield scrapy.Request(
            url='https://hbcg.vn/report/congress_news.html',
            callback=self.parse_bctc
        )


        # yield scrapy.Request(
        #     url,
        #     meta={
        #         "playwright": True,
        #         "playwright_include_page": True,
        #         "playwright_page_methods": [
        #             # 1. Đợi nút lọc năm xuất hiện
        #             PageMethod("wait_for_selector", "div.years-filter button.dropdown-toggle"),
                    
        #             # 2. Click mở menu
        #             PageMethod("click", "div.years-filter button.dropdown-toggle"),
                    
        #             # 3. Sử dụng XPath cụ thể để click vào năm 2025 (Bỏ qua bước wait_for_selector ul)
        #             # Cách này sẽ tìm chính xác thẻ a chứa text 2025 bên trong khối lọc năm
        #             PageMethod("click", f"//div[contains(@class, 'years-filter')]//a[contains(text(), '{year}')]"),
                    
        #             # 4. Đợi dữ liệu tải
        #             PageMethod("wait_for_load_state", "networkidle"),
        #         ],
        #     },
        #     callback=self.parse_bctc
        # )
        # yield scrapy.Request(
        #     url,
        #     meta={
        #         "playwright": True,
        #         "playwright_include_page": True,
        #         "playwright_page_methods": [
        #             # 1. Đợi nút lọc năm xuất hiện
        #             PageMethod("wait_for_selector", "div.years-filter button.dropdown-toggle"),
                    
        #             # 2. Click mở menu
        #             PageMethod("click", "div.years-filter button.dropdown-toggle"),
                    
        #             # 3. Sử dụng XPath cụ thể để click vào năm 2025 (Bỏ qua bước wait_for_selector ul)
        #             # Cách này sẽ tìm chính xác thẻ a chứa text 2025 bên trong khối lọc năm
        #             PageMethod("click", f"//div[contains(@class, 'years-filter')]//a[contains(text(), '{year-1}')]"),
                    
        #             # 4. Đợi dữ liệu tải
        #             PageMethod("wait_for_load_state", "networkidle"),
        #         ],
        #     },
        #     callback=self.parse_bctc
        # )

        # yield scrapy.Request(
        #     url= 'https://hbcg.vn/report/congress_news.html',
        #     meta={
        #         "playwright": True,
        #         "playwright_include_page": True,
        #         "playwright_page_methods": [
        #             # 1. Đợi nút lọc năm xuất hiện
        #             PageMethod("wait_for_selector", "div.years-filter button.dropdown-toggle"),
                    
        #             # 2. Click mở menu
        #             PageMethod("click", "div.years-filter button.dropdown-toggle"),
                    
        #             # 3. Sử dụng XPath cụ thể để click vào năm 2025 (Bỏ qua bước wait_for_selector ul)
        #             # Cách này sẽ tìm chính xác thẻ a chứa text 2025 bên trong khối lọc năm
        #             PageMethod("click", f"//div[contains(@class, 'years-filter')]//a[contains(text(), '{year}')]"),
                    
        #             # 4. Đợi dữ liệu tải
        #             PageMethod("wait_for_load_state", "networkidle"),
        #         ],
        #     },
        #     callback=self.parse_bctc
        # )
        # yield scrapy.Request(
        #     url= 'https://hbcg.vn/report/congress_news.html',
        #     meta={
        #         "playwright": True,
        #         "playwright_include_page": True,
        #         "playwright_page_methods": [
        #             # 1. Đợi nút lọc năm xuất hiện
        #             PageMethod("wait_for_selector", "div.years-filter button.dropdown-toggle"),
                    
        #             # 2. Click mở menu
        #             PageMethod("click", "div.years-filter button.dropdown-toggle"),
                    
        #             # 3. Sử dụng XPath cụ thể để click vào năm 2025 (Bỏ qua bước wait_for_selector ul)
        #             # Cách này sẽ tìm chính xác thẻ a chứa text 2025 bên trong khối lọc năm
        #             PageMethod("click", f"//div[contains(@class, 'years-filter')]//a[contains(text(), '{year-1}')]"),
                    
        #             # 4. Đợi dữ liệu tải
        #             PageMethod("wait_for_load_state", "networkidle"),
        #         ],
        #     },
        #     callback=self.parse_bctc
        # )
    async def parse_bctc(self, response):
        # Lưu file để kiểm tra giao diện
        # with open("debug_page.html", "wb") as f:
        #     f.write(response.body)
        
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

        # 2. Lấy danh sách các khối tin tức
        reports = response.xpath('//div[contains(@class, "gridBlock-content")]')
    
        for report in reports:
            # Trích xuất dữ liệu
            title = report.xpath('.//p[contains(@class, "txt7")]/text()').get()
            summary_desc = report.xpath('.//p[contains(@class, "gridBlock-description")]/text()').get()
            date_raw = report.xpath('.//p[@class="date-info"]/text()').get()
            if date_raw:
                # "Cập nhật ngày: 30/10/2025" -> "30/10/2025"
                clean_date = date_raw.split(":")[-1].strip()
            
            pdf_url = report.xpath('./a/@href').get()

            if not title:
                continue

            # Làm sạch dữ liệu
            cleaned_title = title.strip()
            # Xử lý chuỗi "Cập nhật ngày: DD/MM/YYYY"
            iso_date = convert_date_to_iso8601(clean_date)
            full_pdf_url = response.urljoin(pdf_url)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Tạo ID duy nhất dựa trên tiêu đề và ngày
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
            e_item['details_raw'] = f"{cleaned_title}\n{summary_desc if summary_desc else ''}\nLink: {full_pdf_url}"
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