import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_eme'
    mcpcty= 'EME'

    # Thay thế bằng domain thực tế
    allowed_domains = ['emec.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['http://emec.vn/thong-bao/vn'] 

    # def start_requests(self):
    #     yield scrapy.Request(
    #     url=self.start_urls[0],
    #     callback=self.parse,
    #     # Thêm meta để kích hoạt Playwright
    #     meta={'playwright': True}
    # )
    
    def parse(self, response):
        # Chọn tất cả các khối lớn chứa danh mục (productst-e)
        post_blocks = response.css('div.bp-content')

        for block in post_blocks:
            # 1. Trích xuất Tiêu đề chính của bài viết
            # Tiêu đề nằm trong <a> trong class 'cap-bp' hoặc trong <h3> trong 'sph-content'
            title_element = block.css('.cap-bp span a')
            title = title_element.css('::attr(title)').get()
            
            # 2. Trích xuất Liên kết URL chi tiết bài viết
            link_detail = title_element.css('::attr(href)').get()

            # 3. Trích xuất Ngày và Giờ công bố
            # Dùng selector để lấy text của span thứ hai trong 'cap-bp'
            # 3. Trích xuất Ngày và Giờ công bố
            datetime_parts = block.css('.cap-bp span:nth-child(2)::text').getall()

            # Nối tất cả các phần lại và làm sạch
            # Các node text sẽ là: [' ', '04/12/2025 02:07']
            raw_datetime = "".join(datetime_parts)

            # Làm sạch để loại bỏ khoảng trắng ở đầu và cuối chuỗi
            cleaned_datetime = raw_datetime.strip() if raw_datetime else None
            
            # 4. Trích xuất Tên file PDF/Tài liệu đính kèm
            # Tên file nằm trong thẻ <h4>, dùng ::text để lấy nội dung, strip() để loại bỏ các ký tự xuống dòng và khoảng trắng
            file_name_raw = block.css('.sph-content h4::text').getall()
            
            # Xử lý làm sạch dữ liệu
            cleaned_title = title.strip() if title else None
            
            # Xử lý tên file (có thể có nhiều file đính kèm, mỗi file là một dòng text)
            file_names = []
            if file_name_raw:
                for name in file_name_raw:
                    clean_name = name.strip()
                    if clean_name:
                        file_names.append(clean_name)

            e_item = EventItem()
            # 2. Trích xuất dữ liệu chi tiết
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['date'] = convert_date_to_iso8601(cleaned_datetime)
            e_item['summary'] = cleaned_title
            
            e_item['details_raw'] = str(cleaned_title) + '\n' + str(link_detail) 
                        
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
    input_format = '%d/%m/%Y %H:%M'
    
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
