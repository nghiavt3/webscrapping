import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re

# Giữ nguyên hàm chuyển đổi ngày tháng vì nó hợp lệ
def convert_date_to_iso8601(vietnam_date_str):
    """
    Chuyển đổi chuỗi ngày tháng từ định dạng 'DD/MM/YYYY' sang 'YYYY-MM-DD' (ISO 8601).
    """
    if not vietnam_date_str:
        return None
    input_format = '%d/%m/%Y'
    output_format = '%Y-%m-%d'

    try:
        # Xử lý chuỗi date có thể là tuple hoặc chứa dấu phẩy/khoảng trắng
        cleaned_date_str = str(vietnam_date_str).replace(',', '').replace('(', '').replace(')', '').strip()
        
        date_object = datetime.strptime(cleaned_date_str, input_format)
        iso_date_str = date_object.strftime(output_format)
        
        return iso_date_str
    
    except ValueError as e:
        print(f"⚠️ Lỗi chuyển đổi ngày tháng '{vietnam_date_str}' (phải là DD/MM/YYYY): {e}")
        return None

class EventSpider(scrapy.Spider):
    name = 'event_aas'
    allowed_domains = ['aas.com.vn'] 
    start_urls = ['https://aas.com.vn/danh-muc-thong-tin-co-dong/cong-bo-thong-tin/'] 

    def parse(self, response):
        # SỬA LỖI MỚI: Sử dụng XPath dựa trên vị trí của container chính
        # Tìm thẻ div chứa cả featured article và list articles
        # Dùng contains(@class, "grid") và contains(@class, "gap-6") nhưng kiểm tra lại tính chính xác
        
        # Thử XPath dựa trên div chứa tất cả các bài viết:
        # Nếu trang web dùng Playwright, selector này phải hoạt động khi nội dung được tải.
        container = response.xpath('//div[contains(@class, "grid grid-cols-1 xl:grid-cols-2")]')
        
        if not container:
            # Thử phương pháp dò tìm nếu selector trên vẫn không hoạt động
            # Thử tìm thẻ cha của một phần tử ổn định, ví dụ: tìm thẻ cha của tiêu đề "CÔNG BỐ THÔNG TIN"
            container = response.xpath('//h1[text()="CÔNG BỐ THÔNG TIN"]/following-sibling::div[1]')
            if not container:
                 self.logger.error("🚫 LỖI: Không tìm thấy container bài viết chính. Kiểm tra lại XPath.")
                 return

        # Lấy container chính (chắc chắn chỉ lấy phần tử đầu tiên nếu có nhiều kết quả)
        container = container.get()

        # =========================================================
        # 1. Trích xuất Bài viết Nổi bật (Featured Article - Cột 1)
        # Sử dụng CSS Selector đơn giản: .cbtt-cus.first-item
        featured_item = container.css('.cbtt-cus.first-item')
        if featured_item:
            yield self.extract_item(featured_item, is_featured=True)

        # =========================================================
        # 2. Trích xuất Danh sách Bài viết khác (List Articles - Cột 2)
        # Chọn div thứ 2 (cột danh sách) trong container chính
        list_container = container.xpath('./div[2]')
        
        # Chọn các mục bài viết con trực tiếp trong list_container
        list_items = list_container.xpath('./div[contains(@class, "flex flex-col sm:flex-row")]')
        
        for item in list_items:
            yield self.extract_item(item, is_featured=False)
            
    def extract_item(self, selector, is_featured=False):
        
        e_item = EventItem()
        e_item['mcp'] = 'AAS'
        e_item['web_source'] = self.allowed_domains[0]
        
        # === Ngày đăng ===
        # Lấy ngày (cùng selector cho cả featured và list)
        date_raw = selector.css('div.flex.items-center.gap-2 p::text').get() 
        e_item['date'] = convert_date_to_iso8601(date_raw)
        
        # === Tiêu đề & Tóm tắt & Link ===
        if is_featured:
            # Bài viết nổi bật (Featured)
            title_raw = selector.css('h2.truncate-1row::text').get()
            summary_raw = selector.css('p.truncate-2row::text').get()
            url_raw = selector.css('a.link-yellow::attr(href)').get()
        else:
            # Bài viết danh sách (List)
            # Dùng CSS Selector cho Title và Tóm tắt vì nó ngắn và dễ đọc hơn khi đã khắc phục lỗi cú pháp
            title_raw = selector.css('a.text-body-md-semibold::text').get()
            summary_raw = selector.css('div.text-body-md-regular.text-text-content::text').get()
            url_raw = selector.css('a.link-yellow::attr(href)').get()
        
        # Làm sạch dữ liệu và gán vào item
        e_item['summary'] = title_raw.strip() if title_raw else None
        summary_cleaned = summary_raw.strip() if summary_raw else None
        e_item['details_raw'] = (e_item['summary'] or '') + '\n' + (summary_cleaned or '') + '\n' + (url_raw or '')
        
        return e_item