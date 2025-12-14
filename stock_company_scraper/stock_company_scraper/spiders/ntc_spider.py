import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_ntc'
    mcpcty = 'NTC'
    # Thay thế bằng domain thực tế
    allowed_domains = ['namtanuyen.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://namtanuyen.com.vn/danh-muc/thong-bao-co-dong'] 

    def parse(self, response):
        # Chọn tất cả các khối tin tức (các div.row có chứa border-bottom style)
        # Loại trừ các div.row không có nội dung chính thức (ví dụ: title row, empty rows)
        news_blocks = response.css('div.container > div.row[style*="border-bottom"]')
        
        for block in news_blocks:
            # 1. Trích xuất Tiêu đề và URL
            link_tag = block.css('div.col-8 a')
            tieu_de = link_tag.css('::text').get()
            
            # Lấy URL tương đối và chuyển thành URL tuyệt đối (dùng response.urljoin)
            relative_url = link_tag.css('::attr(href)').get()
            pdf_url = response.urljoin(relative_url) if relative_url else None
            
            # 2. Trích xuất Ngày/Giờ công bố
            # Dùng ::text để lấy nội dung văn bản trực tiếp trong div.col-4
            datetime_str = block.css('div.col-4::text').get()
            
            # Làm sạch dữ liệu (loại bỏ khoảng trắng thừa, xuống dòng, tab)
            if tieu_de:
                tieu_de = tieu_de.strip()
            if datetime_str:
                datetime_str = datetime_str.strip()
            

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = tieu_de
            e_item['details_raw'] = str(tieu_de) + '\n' + str(pdf_url)
            e_item['date'] = convert_date_to_iso8601(datetime_str)
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
    input_format = '%d/%m/%Y %H:%M:%S'    
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
