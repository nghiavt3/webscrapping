import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_bvh'
    mcpcty= 'BVH'

    # Thay thế bằng domain thực tế
    allowed_domains = ['baoviet.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://baoviet.com.vn/vi/quan-he-co-dong'] 

    # def start_requests(self):
    #     yield scrapy.Request(
    #     url=self.start_urls[0],
    #     callback=self.parse,
    #     # Thêm meta để kích hoạt Playwright
    #     meta={'playwright': True}
    # )
    
    def parse(self, response):
       # Duyệt qua từng nhóm Quý
        for quarter_group in response.css('.item.accordion'):
            quarter_name = quarter_group.css('.item__head h3::text').get()
            # Làm sạch dữ liệu (loại bỏ khoảng trắng thừa)
            quarter_name = quarter_name.strip() if quarter_name else "N/A"

            # Duyệt qua từng bài đăng trong Quý đó
            for panel in quarter_group.css('.f-panel'):
                post_title = panel.css('.post__title::text').get()
                post_date = panel.css('.post__date time::text').get()
                
                # Trích xuất danh sách file đính kèm
                attachments = []
                for file_li in panel.css('ul.item-list li'):
                    file_name = "".join(file_li.css('a ::text').getall()).strip()
                    file_url = file_li.css('a::attr(href)').get()
                    
                    attachments.append({
                        'file_name': file_name,
                        'file_url': response.urljoin(file_url) # Tạo link tuyệt đối
                    })

                e_item = EventItem()
                # 2. Trích xuất dữ liệu chi tiết
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['date'] = convert_date_to_iso8601(post_date)
                e_item['summary'] = post_title
                
                e_item['details_raw'] = str(post_title) + '\n' + str(attachments) 
                            
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
