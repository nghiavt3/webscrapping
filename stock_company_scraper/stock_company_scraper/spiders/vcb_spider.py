import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
from scrapy_playwright.page import PageMethod
import re
class EventSpider(scrapy.Spider):
    name = 'event_vcb'
    mcpcty = 'VCB'
    # Thay thế bằng domain thực tế
    allowed_domains = ['vietcombank.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.vietcombank.com.vn/vi-VN/Nha-dau-tu'] 

    def start_requests(self):
        url = "https://www.vietcombank.com.vn/vi-VN/Nha-dau-tu"
        yield scrapy.Request(
            url,
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_methods": [
                    # Cách viết đúng: Đợi mạng rảnh (tải xong JS/CSS)
                    PageMethod("wait_for_load_state", state="networkidle"),
                    # Đợi cho đến khi các dòng tin tức (li) thực sự xuất hiện trong DOM
                    PageMethod("wait_for_selector", "li.newest-invest-info__info-item"),
                    # Nghỉ thêm 2 giây để chắc chắn nội dung văn bản đã render xong
                    PageMethod("wait_for_timeout", 2000),
                ],
            },
            callback=self.parse
        )
        
    async def parse(self, response):
        page = response.meta["playwright_page"]
        # Thử lấy danh sách các mục tin tức
        items = response.css('li.newest-invest-info__info-item')
        self.logger.info(f"Tìm thấy {len(items)} mục tin tức trên trang.")

        if len(items) == 0:
            # Nếu vẫn không thấy, chụp ảnh màn hình để xem trang đang hiển thị gì (Debug)
            await page.screenshot(path="debug_vcb.png")
            self.logger.warning("Không tìm thấy item nào. Đã chụp ảnh debug_vcb.png")

        for item in items:
            # VCB thường để tiêu đề trong thẻ p hoặc div bên trong .content-wrap
            raw_text = item.css('div.content-wrap p::text').get() or item.css('div.content-wrap::text').get()
            download_path = item.css('::attr(data-download-url)').get()
            
            if raw_text:
                publish_date, clean_title = clean_and_format_date(raw_text)

                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = clean_title
                e_item['details_raw'] = str(clean_title) + '\n'+ str(response.urljoin(download_path) if download_path else None)
                e_item['date'] = (publish_date)         
                yield e_item

from datetime import datetime

def clean_and_format_date( text):
        if not text:
            return None, None
        
        # Regex tìm ngày dạng (dd/mm/yyyy) ở cuối chuỗi
        date_match = re.search(r'\((\d{2}/\d{2}/\d{4})\)\s*$', text.strip())
        
        if date_match:
            date_str = date_match.group(1)
            # Tách tiêu đề bằng cách loại bỏ phần ngày tháng
            title = text.replace(f"({date_str})", "").strip()
            try:
                # Chuyển đổi 23/12/2025 -> 2025-12-23
                iso_date = datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
                return iso_date, title
            except ValueError:
                return None, text.strip()
        
        return None, text.strip()