import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_ccc'
    mcpcty = 'CCC'
    # Thay thế bằng domain thực tế
    allowed_domains = ['cdcxd.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://cdcxd.com.vn/cong-bo-thong-tin/'] 

    def parse(self, response):
        # 1. Chọn tất cả các hàng dữ liệu (<tr>) trong bảng có class 'download-file', 
        # loại trừ hàng tiêu đề (title-table).
        rows = response.css('table.download-file tr:not(.title-table)')
        
        for row in rows:
            # 2. Trích xuất Tiêu đề Văn bản
            # Title nằm trong <h4><a>::text</a></h4> thuộc <td> đầu tiên
            document_title = row.css('td:nth-child(1) h4.document-title a::text').get()
            
            # 3. Trích xuất Ngày phát hành
            # Ngày nằm trong <td> thứ hai
            release_date = row.css('td:nth-child(2)::text').get().strip()
            
            # 4. Trích xuất Đường dẫn Tải về (URL)
            # Đường dẫn là thuộc tính 'href' của thẻ <a> trong <td> thứ ba
            download_url = row.css('td:nth-child(3) a::attr(href)').get()
            
            # Nếu cần, làm sạch khoảng trắng thừa cho tiêu đề
            if document_title:
                document_title = document_title.strip()
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = document_title
            e_item['details_raw'] = str(document_title) +'\n' + str(download_url)
            e_item['date'] = convert_date_to_iso8601(release_date)
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
