import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_sip'
    mcpcty = 'SIP'
    # Thay thế bằng domain thực tế
    allowed_domains = ['saigonvrg.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://saigonvrg.com.vn/vi/thong-bao-co-dong'] 

    def parse(self, response):
        data_blocks = response.css('div.khungdl > div.dulieu:not(.dulieu2)')
        
        for block in data_blocks:
            # 1. Trích xuất Ngày tạo
            ngay_tao = block.css('p.ngay::text').get().strip()

            # 2. Trích xuất Tiêu đề Bài viết và ID
            title_tag = block.css('h3 a.clickxemdulieu')
            tieu_de = title_tag.css('::text').get().strip()
            data_id = title_tag.css('::attr(data-id)').get()

            # 3. Trích xuất ID từ liên kết Xem (được dùng để tạo URL)
            # Giả sử ID này là quan trọng để xây dựng liên kết xem/tải về
            view_link_id = block.css('p.tttin a.xem::attr(data-id)').get()

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = tieu_de
            e_item['details_raw'] = str(tieu_de) +'\n' + str(view_link_id)
            e_item['date'] = convert_date_to_iso8601(ngay_tao)
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
