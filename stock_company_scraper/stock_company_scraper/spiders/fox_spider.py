import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_fox'
    mcpcty = 'FOX'
    # Thay thế bằng domain thực tế
    allowed_domains = ['fpt.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://fpt.vn/vi/ve-fpt-telecom/quan-he-co-dong/thong-bao-khac','https://fpt.vn/vi/ve-fpt-telecom/quan-he-co-dong/thong-bao-tra-co-tuc'] 
    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
    }
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
            url=url,
            callback=self.parse,
            # Thêm meta để kích hoạt Playwright
            meta={'playwright': True}
    )
    def parse(self, response):
        rows = response.css('#ajax-container table.table tbody tr.table-row')
        
        if not rows:
            self.logger.warning("Không tìm thấy hàng dữ liệu nào (tr.table-row). Vui lòng kiểm tra lại URL hoặc cấu trúc HTML.")
            return

        for row in rows:
            # 2. Trích xuất Tiêu đề/Nội dung (Cột 1)
            # Lấy văn bản từ thẻ <td> đầu tiên
            title = row.css('td:nth-child(1)::text').get()
            
            # 3. Trích xuất Ngày đăng (Cột 2)
            # Lấy văn bản từ thẻ <td> thứ hai
            date_time = row.css('td:nth-child(2)::text').get()
            
            # 4. Trích xuất Link xem online (Cột 3)
            # Link nằm trong thuộc tính href của thẻ <a> có class 'view-pdf' trong td thứ ba
            view_link = row.css('td:nth-child(3) a.view-pdf::attr(href)').get()
            
            # 5. Trích xuất Link tải về (Cột 4)
            # Link nằm trong thuộc tính href của thẻ <a> có class 'img-download' trong td thứ tư
            download_link = row.css('td:nth-child(4) a.img-download::attr(href)').get()
            
            # Làm sạch dữ liệu và tách Ngày/Giờ (Tùy chọn)
            title = title.strip() if title else None
            date_time_stripped = date_time.strip() if date_time else None
            
            date_only = None
            time_only = None
            if date_time_stripped and ' ' in date_time_stripped:
                date_only, time_only = date_time_stripped.split(' ', 1)
            else:
                date_only = date_time_stripped
            
            # Trả về dữ liệu

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(view_link) +'\n' + str(download_link)
            e_item['date'] = date_time_stripped            
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
