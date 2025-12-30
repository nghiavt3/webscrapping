import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
from scrapy_playwright.page import PageMethod
class EventSpider(scrapy.Spider):
    name = 'event_msb'
    # Thay thế bằng domain thực tế
    allowed_domains = ['msb.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.msb.com.vn/vi/nha-dau-tu/cong-bo-thong-tin.html'] 

    # custom_settings = {
    #     'CONCURRENT_REQUESTS': 1,
    #     'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
    # }

    async def start(self): # Dùng start thay cho start_requests
        yield scrapy.Request(
            url="https://www.msb.com.vn/vi/nha-dau-tu/cong-bo-thong-tin.html",
            meta={
                "playwright": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_load_state", "networkidle"), # Đợi mạng rảnh mới cào
                    PageMethod("wait_for_timeout", 2000),             # Đợi thêm 2 giây cho chắc
                ],
            },
            callback=self.parse
        )

    def parse(self, response):
        # 1. Lấy danh sách tất cả các thông báo (records)
        bao_cao_items = response.css('.baocao-item')

        for item in bao_cao_items:
            # 1. Trích xuất Ngày công bố
            # Chọn thẻ <p> và lấy nội dung văn bản sau dấu hai chấm (:)
            # Selector: p::text (sẽ trả về 'Ngày công bố: 02/12/2025')
            date_raw = item.css('p::text').get()
            
            # Xử lý chuỗi để chỉ lấy ngày (ví dụ: '02/12/2025')
            ngay_cong_bo = date_raw.replace('Ngày công bố:', '').strip() if date_raw else None
            
            # 2. Trích xuất Tiêu đề
            # Chọn thẻ <h3> và lấy nội dung văn bản
            # Selector: h3::text
            tieu_de = item.css('h3::text').get().strip() if item.css('h3::text').get() else None
            
            # 3. Trích xuất URL Tải về (đường link của tệp PDF)
            # Chọn thẻ <a> bên trong div.d-flex và lấy thuộc tính 'href'
            # Selector: div.d-flex a::attr(href)
            url_tai_ve_relative = item.css('div.d-flex a::attr(href)').get()
            
            # Chuyển URL tương đối thành URL tuyệt đối (quan trọng)
            url_tai_ve_absolute = response.urljoin(url_tai_ve_relative) if url_tai_ve_relative else None

            e_item = EventItem()
            e_item['mcp'] = 'MSB'
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = tieu_de
            e_item['details_raw'] = str(tieu_de) +'\n' + str(url_tai_ve_absolute)
            e_item['date'] = convert_date_to_iso8601(ngay_cong_bo)               
            yield e_item

from datetime import datetime

def convert_date_to_iso8601(vietnam_date_str):
    """
    Chuyển đổi chuỗi ngày tháng từ định dạng 'DD/MM/YYYY' sang 'YYYY-MM-DD' (ISO 8601).
    
    :param vietnam_date_str: Chuỗi ngày tháng đầu vào, ví dụ: '20/09/2025'
    :return: Chuỗi ngày tháng ISO 8601, ví dụ: '2025-09-20', hoặc None nếu có lỗi.
    """
    if not vietnam_date_str:
        return None

    # Định dạng đầu vào: Ngày/Tháng/Năm ('%d/%m/%Y')
    input_format = '%d/%m/%Y'
    
    # Định dạng đầu ra: Năm-Tháng-Ngày ('%Y-%m-%d') - chuẩn ISO 8601 cho ngày
    output_format = '%Y-%m-%d'

    try:
        # 1. Parse chuỗi đầu vào thành đối tượng datetime
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        
        # 2. Định dạng lại đối tượng datetime thành chuỗi ISO 8601
        iso_date_str = date_object.strftime(output_format)
        
        return iso_date_str
    
    except ValueError as e:
        print(f"⚠️ Lỗi chuyển đổi ngày tháng '{vietnam_date_str}' (phải là DD/MM/YYYY): {e}")
        return None
