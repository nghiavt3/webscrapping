import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_yeg'
    mcpcty = 'YEG'
    # Thay thế bằng domain thực tế
    allowed_domains = ['yeah1group.com'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://yeah1group.com/investor-relation/announcements'] 

    def parse(self, response):
        # 1. Selector chính: Tìm tất cả các mục thông báo
        # Mỗi thông báo nằm trong một thẻ <li> với class là 'py-6 border-b border-[#CCCCCC]'
        announcements = response.css('ul.w-full > li')
        
        for item in announcements:
            # 2. Trích xuất Tiêu đề
            # H4 nằm sâu trong div, có thể chọn trực tiếp bằng h4
            title = item.css('h4::text').get().strip()
            
            # 3. Trích xuất Ngày/Giờ
            # Thẻ span chứa ngày giờ có class 'text-[#475467] text-sm'
            datetime_raw = item.css('span.order-last.text-sm::text').get()
            
            # 4. Trích xuất Kích thước file (size)
            # Thẻ span chứa size có class 'size text-[#475467] text-sm'
            size = item.css('span.size::text').get()
            
            # 5. Trích xuất Link Download PDF
            # Link nằm trong thẻ <a> đầu tiên có class 'ir__link' trong khu vực download
            # LƯU Ý: Có 2 thẻ <a> với class 'ir__link' trong khu vực download. 
            # Chúng ta sẽ lấy thẻ <a> đầu tiên để lấy thuộc tính href.
            
            # Tìm thẻ span cha chứa các link download
            download_area = item.css('span.download__icon')
            
            # Lấy thuộc tính href của thẻ <a> đầu tiên bên trong download_area
            # Selector: a.ir__link:nth-child(1) là thẻ <a> đầu tiên
            link = download_area.css('a.ir__link::attr(href)').get()

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(link)
            e_item['date'] = convert_date_to_iso8601(datetime_raw)               
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
    input_format = '%H:%M %d/%m/%Y'
    
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
