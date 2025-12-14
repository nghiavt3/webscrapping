import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_nvl'
    mcpcty = 'NVL'
    # Thay thế bằng domain thực tế
    allowed_domains = ['novaland.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.novaland.com.vn/quan-he-dau-tu/cong-bo-thong-tin/thong-bao'] 

    def parse(self, response):
        data_rows = response.css('div.block-shareHoldersList table.table tbody tr:nth-child(n+2)')
        
        for row in data_rows:
            # Các cột được xác định theo vị trí <td>
            
            # 2. Tên văn bản (Cột 1: <td> đầu tiên)
            # Chọn thẻ <a> bên trong <td> thứ nhất và lấy nội dung văn bản.
            document_title = row.css('td:nth-child(1) a::text').get().strip()

            # 3. Ngày ban hành (Cột 2: <td> thứ hai)
            # Lấy nội dung văn bản trực tiếp trong <td> thứ hai.
            issue_date = row.css('td:nth-child(2)::text').get().strip()
            
            # 4. Đường dẫn tải về (Cột 3: <td> thứ ba)
            # Chọn thẻ <a> bên trong <td> thứ ba và lấy thuộc tính href.
            relative_download_url = row.css('td:nth-child(3) a::attr(href)').get()
            
            # Xây dựng URL đầy đủ (Absolute URL)
            absolute_download_url = response.urljoin(relative_download_url) if relative_download_url else None

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = document_title
            e_item['details_raw'] = str(document_title) + '\n' + str(absolute_download_url)
            e_item['date'] = convert_date_to_iso8601(issue_date)
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
    input_format = "%d/%m/%Y"    
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
