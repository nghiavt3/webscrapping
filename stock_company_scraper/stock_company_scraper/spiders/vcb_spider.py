import scrapy
import sqlite3
import re
from stock_company_scraper.items import EventItem
from datetime import datetime
from scrapy_playwright.page import PageMethod

class EventSpider(scrapy.Spider):
    name = 'event_vcb'
    mcpcty = 'VCB'
    allowed_domains = ['vietcombank.com.vn'] 
    start_urls = ['https://www.vietcombank.com.vn/vi-VN/Nha-dau-tu'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        url = self.start_urls[0]
        yield scrapy.Request(
            url,
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_methods": [
                    # Đợi mạng rảnh để đảm bảo các API nội bộ đã gọi xong
                    PageMethod("wait_for_load_state", state="networkidle"),
                    # Đợi selector tin tức xuất hiện
                    PageMethod("wait_for_selector", "li.newest-invest-info__info-item"),
                    # Nghỉ thêm để nội dung văn bản render hoàn tất
                    PageMethod("wait_for_timeout", 2000),
                ],
            },
            callback=self.parse
        )
        
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

        page = response.meta["playwright_page"]
        items = response.css('li.newest-invest-info__info-item')

        if len(items) == 0:
            await page.screenshot(path="debug_vcb.png")
            self.logger.warning("Không tìm thấy item nào. Kiểm tra debug_vcb.png")
            return

        for item in items:
            # VCB thường gộp tiêu đề và ngày vào một chuỗi: "Tiêu đề... (DD/MM/YYYY)"
            raw_text = item.css('div.content-wrap p::text').get() or item.css('div.content-wrap::text').get()
            download_path = item.css('::attr(data-download-url)').get()
            
            if not raw_text:
                continue

            # Xử lý tách ngày và tiêu đề bằng Regex
            publish_date, clean_title = clean_and_format_date(raw_text)
            
            if not publish_date:
                publish_date = datetime.now().strftime('%Y-%m-%d')

            summary = clean_title if clean_title else "Thông báo nhà đầu tư"

            # -------------------------------------------------------
            # 2. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{publish_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT.")
                break 

            # 3. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['date'] = publish_date
            
            full_url = response.urljoin(download_path) if download_path else "N/A"
            e_item['details_raw'] = f"{summary}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()
        await page.close()

def clean_and_format_date(text):
    if not text:
        return None, None
    
    # Regex tìm (dd/mm/yyyy) ở cuối chuỗi
    date_match = re.search(r'\((\d{2}/\d{2}/\d{4})\)\s*$', text.strip())
    
    if date_match:
        date_str = date_match.group(1)
        title = text.replace(f"({date_str})", "").strip()
        try:
            iso_date = datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
            return iso_date, title
        except ValueError:
            return None, text.strip()
    
    return None, text.strip()