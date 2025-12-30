import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_tpb'
    mcpcty = 'TPB'
    # Thay thế bằng domain thực tế
    allowed_domains = ['tpb.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://tpb.vn/nha-dau-tu/thong-bao-co-dong'] 

    def start_requests(self):
        yield scrapy.Request(
        url=self.start_urls[0],
        callback=self.parse,
        # Thêm meta để kích hoạt Playwright
        meta={"playwright": True,
                
                }
    )
        
    def parse(self, response):
        # Chọn tất cả các khối bao quanh
        items = response.css('.b-right-download')

        for item in items:
            # 1. Lấy link file
            file_url = item.css('a::attr(href)').get()
            
            # 2. Lấy toàn bộ text trong span
            raw_text = item.css('span::text').get()
            
            if raw_text:
                # Làm sạch khoảng trắng thừa và xuống dòng
                clean_text = " ".join(raw_text.split())
                
                # Tách ngày (10 ký tự đầu: DD/MM/YYYY) và phần nội dung còn lại
                publish_date = clean_text[:10]  # Lấy "19/12/2025"
                title = clean_text[10:].strip() # Lấy phần chữ phía sau
            else:
                publish_date = None
                title = None

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(response.urljoin(file_url) if file_url else None)
            e_item['date'] = convert_date_to_iso8601(publish_date)               
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
