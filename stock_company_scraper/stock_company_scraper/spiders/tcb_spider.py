import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_tcb'
    mcpcty= 'TCB'

    # Thay thế bằng domain thực tế
    allowed_domains = ['techcombank.com'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://techcombank.com/nha-dau-tu/cong-bo-thong-tin/tai-lieu-doanh-nghiep'] 

    def start_requests(self):
        yield scrapy.Request(
        url=self.start_urls[0],
        callback=self.parse,
        # Thêm meta để kích hoạt Playwright
        meta={'playwright': True}
    )
    
    def parse(self, response):
        # Chọn tất cả các thẻ <tr> chứa tin tức
       for row in response.css('div.row'):
            # Trích xuất ngày tháng và xóa khoảng trắng thừa
            date= row.css('.date span::text').get(default='').strip()
            
            # Trích xuất tiêu đề bài viết
            title= row.css('.content h4::text').get(default='').strip()
            
            # Trích xuất link download (lấy từ thẻ a có class link trong vùng show-document)
            download_link= response.urljoin(
                row.css('.file-download .show-document a.link::attr(href)').get()
            ),
            
            # Trích xuất tên file từ thuộc tính download của tcb-tracker (nếu cần)
            file_name= row.css('.file-download .show-document tcb-tracker::attr(download)').get()

            e_item = EventItem()
            # 2. Trích xuất dữ liệu chi tiết
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['date'] = convert_date_to_iso8601(date)
            e_item['summary'] = title
            
            e_item['details_raw'] = str(title) + '\n' + str(download_link) 
                         
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
