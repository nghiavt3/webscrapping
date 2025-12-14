import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_thg'
    mcpcty = 'THG'
    # Thay thế bằng domain thực tế
    allowed_domains = ['ticco.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://ticco.com.vn/quan-he-co-dong/cong-bo-thong-tin/'] 

    def parse(self, response):
        # 1. Lấy danh sách tất cả các thông báo (records)
        items = response.css('div.list.grid > div.item')

        for item in items:
            # 2. Trích xuất Ngày công bố (dd.mm.yyyy)
            # Selector: time (element bên trong item)
            published_date = item.css('time::text').get().strip()
            
            # 3. Trích xuất Tiêu đề bài viết
            # Selector: span.line-clamp-3 (bên trong div.title)
            title = item.css('div.title span.line-clamp-3::text').get().strip()

            # 4. Trích xuất URL File đính kèm (PDF)
            # Phải đi sâu vào phần ẩn (div.hidden > div#popup-document-*)
            # Selector: Lấy href của thẻ <a> nằm trong div.content trong popup
            
            # Lấy giá trị data-src (ví dụ: #popup-document-3672) của thẻ <a> kích hoạt popup
            data_src = item.css('a.download-file::attr(data-src)').get()
            
            # Xử lý: data_src sẽ là '#popup-document-3672'. 
            # Ta cần dùng nó để tìm thẻ <div> ẩn có id tương ứng.
            
            file_url = None
            if data_src:
                # Bỏ ký tự '#' và dùng làm ID selector trong response (hoặc item nếu không muốn dùng response)
                # Selector cho URL PDF: div#ID_CUA_POPUP > div.wrapper div.content a::attr(href)
                # Vì các popup nằm trong thẻ div.hidden (anh em với div.item), 
                # ta phải tìm chúng trong toàn bộ response (hoặc từ thẻ chứa tất cả items: div#list)
                
                # Giả sử chúng ta đang ở scope là response (hàm parse)
                # Dùng response.css vì các popup nằm ngoài phạm vi item hiện tại
                file_url = response.css(f'{data_src} div.content a::attr(href)').get()

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(file_url)
            e_item['date'] = convert_date_to_iso8601(published_date)               
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
    input_format = '%d.%m.%Y'
    
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
