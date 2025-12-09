import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_plc'
    mcpcty = 'PLC'
    # Thay thế bằng domain thực tế
    allowed_domains = ['plc.petrolimex.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://plc.petrolimex.com.vn/nd/tt-co-dong.html'] 

    def parse(self, response):
        articles = response.css('.post-default')

        for article in articles:
            # 2. Trích xuất Tiêu đề Văn bản
            title = article.css('h3.post-default__title a::text').get(default='').strip()

            # 3. Trích xuất Liên kết URL (chúng ta cần nối với base URL nếu là link tương đối)
            relative_url = article.css('h3.post-default__title a::attr(href)').get(default='')
            full_url = response.urljoin(relative_url)

            # 4. Trích xuất Danh mục
            category = article.css('.meta-cat a::text').get(default='').strip()

            # 5. Trích xuất Ngày đăng
            # Lấy tất cả các nút văn bản trong .post-default__meta, sau đó lọc chuỗi Ngày
            meta_parts = article.css('.post-default__meta::text').getall()
            
            date = ''
            if meta_parts:
                # Tìm chuỗi chứa ngày, thường là phần tử cuối cùng hoặc phần tử có chứa dấu '|'
                for part in meta_parts:
                    cleaned_part = part.replace('&nbsp;', '').replace('|', '').strip()
                    if '/' in cleaned_part: # Giả định ngày có định dạng XX/XX/XXXX
                        date = cleaned_part
                        break
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = title +'\n' + full_url
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
