import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_bcc'
    mcpcty = 'BCC'
    # Thay thế bằng domain thực tế
    allowed_domains = ['ximangbimson.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://ximangbimson.com.vn/quan-he-co-dong/'] 

    def parse(self, response):
        rows = response.css('table tbody tr:not(:first-child)')

        for row in rows:
            # 2. Trích xuất Tiêu đề Văn bản
            title = row.css('td:nth-child(2) a::text').get(default='').strip()
            
            # 3. Trích xuất URL Chi tiết (link của tiêu đề)
            detail_url = row.css('td:nth-child(2) a::attr(href)').get(default='').strip()
            
            # 4. Trích xuất Ngày Ban hành
            issued_date = row.css('td:nth-child(3)::text').get(default='').strip()
            
            # 5. Trích xuất URL Tệp PDF (link tải về)
            # Link nằm trong thẻ <a> của cột thứ 4
            pdf_url_relative = row.css('td:nth-child(4) a::attr(href)').get(default='')
            
            # Đảm bảo URL đầy đủ nếu nó là link tương đối (mặc dù ở đây có vẻ là link tuyệt đối)
            pdf_url_full = response.urljoin(pdf_url_relative)

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(detail_url) +'\n' +  str(pdf_url_full)
            e_item['date'] = convert_date_to_iso8601(issued_date)               
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
