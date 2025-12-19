import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_idj'
    mcpcty = 'IDJ'
    # Thay thế bằng domain thực tế
    allowed_domains = ['idjf.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.idjf.vn/vi/quan-he-co-dong'] 

    def parse(self, response):
        rows = response.css('#tbl-noti tbody tr')

        for row in rows:
            # 2. Trích xuất từng cột (td)
            
            # Cột 1: Tên tài liệu
            # Selector: td đầu tiên ::text
            title = row.css('td:nth-child(1)::text').get().strip()

            # Cột 2: Thời gian đăng
            # Selector: td thứ hai ::text (chú ý dùng .strip() để làm sạch)
            pub_date = row.css('td:nth-child(2)::text').get().strip()

            # Cột 3: URL Tải về
            # Selector: td thứ ba, tìm thẻ a bên trong, lấy thuộc tính href
            # Dùng .get() vì nó chỉ trả về một kết quả (URL)
            download_url_relative = row.css('td:nth-child(3) a::attr(href)').get()
            
            # Xử lý URL tương đối (nếu có) để tạo URL tuyệt đối
            if download_url_relative:
                # response.urljoin() sẽ ghép URL cơ sở với URL tương đối
                download_url_absolute = response.urljoin(download_url_relative)
            else:
                download_url_absolute = None


            # 3. Trả về dữ liệu đã trích xuất

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(download_url_absolute)
            e_item['date'] = convert_date_to_iso8601(pub_date)               
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
