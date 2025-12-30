import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_vre'
    mcpcty = 'VRE'
    # Thay thế bằng domain thực tế
    allowed_domains = ['ir.vincom.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://ir.vincom.com.vn/cong-bo-thong-tin/cong-bo-thong-tin-vi/'] 
    # def start_requests(self):
    #     yield scrapy.Request(
    #     url=self.start_urls[0],
    #     callback=self.parse,
    #     # Thêm meta để kích hoạt Playwright
    #     meta={"playwright": True,               
    #         }
    # )
    def parse(self, response):
        # Duyệt qua từng khối tin tức
        for post in response.css('div.post-list-resource div.column'):
            item_node = post.css('div.item')
            
            # 1. Trích xuất tiêu đề (Title)
            title = item_node.css('h6 a::attr(title)').get()
            if not title:
                title = item_node.css('h6 a::text').get()

            # 2. Trích xuất đường dẫn (URL)
            url = item_node.css('h6 a::attr(href)').get()

            # 3. Trích xuất ngày tháng (Date)
            # Ưu tiên lấy từ thuộc tính datetime để có định dạng chuẩn ISO
            raw_date = item_node.css('time::attr(datetime)').get()
            # Nếu chỉ muốn lấy phần ngày YYYY-MM-DD
            iso_date = raw_date.split('T')[0] if raw_date else None

            if title:
                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = str(title.strip() if title else None)
                e_item['details_raw'] = str(title.strip() if title else None) +'\n' + str(url)
                e_item['date'] = (iso_date)               
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
