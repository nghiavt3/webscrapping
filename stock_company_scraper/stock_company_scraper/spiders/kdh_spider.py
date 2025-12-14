import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_kdh'
    mcpcty = 'KDH'
    # Thay thế bằng domain thực tế
    allowed_domains = ['khangdien.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.khangdien.com.vn/co-dong/cong-bo-thong-tin'] 

    def parse(self, response):
        announcements = response.css('div.stockcol')

        for ann in announcements:
            # Selector cho thẻ <a> chứa tất cả thông tin
            link_selector = ann.css('a')
            
            # 2. Trích xuất URL
            url = link_selector.css('::attr(href)').get()

            # 3. Trích xuất Ngày
            # Ngày nằm trong thẻ <i> bên trong <a>
            date_raw = link_selector.css('i::text').get()
            # Xóa các ký tự ngoặc đơn và khoảng trắng dư thừa
            date = date_raw.strip('() \n') if date_raw else None

            # 4. Trích xuất Tiêu đề
            # Tiêu đề là văn bản trực tiếp trong thẻ <a>, loại trừ nội dung của thẻ <i>.
            # Ta lấy tất cả các text node trực tiếp (::text) và lọc/ghép để có tiêu đề sạch.
            
            title = None
            # Lấy tất cả các text node con trực tiếp của thẻ <a>
            all_a_text_nodes = link_selector.css('::text').getall()
            
            # Lọc và lấy phần tử chứa tiêu đề thực tế (thường là node text lớn nhất sau khi strip)
            title_parts = [t.strip() for t in all_a_text_nodes if t.strip()]
            
            if title_parts:
                # Tiêu đề là phần tử đầu tiên không rỗng sau khi loại bỏ thẻ <i>
                # Tuy nhiên, cần đảm bảo loại bỏ phần văn bản của <i> khỏi title_parts
                # Vì nội dung của <i> đã được trích xuất riêng, chúng ta chỉ cần lấy phần tử đầu tiên 
                # của `title_parts` vì nó chứa tiêu đề chính.
                title = title_parts[0]
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) + '\n' + str(url)
            e_item['date'] = convert_date_to_iso8601(date)
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
