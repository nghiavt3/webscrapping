import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_tci'
    mcpcty = 'TCI'
    # Thay thế bằng domain thực tế
    allowed_domains = ['tcsc.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://tcsc.vn/vi/download/Bao-cao-tai-chinh/','https://tcsc.vn/vi/download/Bao-cao-TLATTC-38/','https://tcsc.vn/vi/download/Bao-cao-thuong-nien/','https://tcsc.vn/vi/download/Bao-cao-quan-tri-Cong-ty/','https://tcsc.vn/vi/download/Hop-Dai-hoi-co-dong/'] 

    def parse(self, response):
        # Chọn tất cả các khối div có class 'Row' (bỏ qua 'divHeading')
        rows = response.css('div.divTable div.Row')

        for row in rows:
            # 1. Trích xuất tất cả các ô 'Cell' trong một hàng
            cells = row.css('div.Cell')
            
            # 2. Trích xuất thông tin từ ô đầu tiên (Tên file & Link)
            # Dùng ::attr(title) để lấy tiêu đề đầy đủ không bị dấu "..."
            title = cells[0].css('a::attr(title)').get() or cells[0].css('a::text').get()
            file_url = cells[0].css('a::attr(href)').get()

            # 3. Trích xuất thông tin từ các ô tiếp theo dựa trên chỉ số (index)
            upload_date = cells[1].css('::text').get()
            file_size = cells[2].css('::text').get()

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(response.urljoin(file_url) if file_url else "")
            e_item['date'] = convert_date_to_iso8601(upload_date.strip() if upload_date else "")               
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
    input_format = "%d/%m/%Y %H:%M"
    
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
