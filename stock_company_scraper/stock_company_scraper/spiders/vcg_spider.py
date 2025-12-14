import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_vcg'
    # Thay thế bằng domain thực tế
    allowed_domains = ['vinaconex.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://vinaconex.com.vn/quan-he-co-dong/thong-tin-chung?page=1'] 

    def parse(self, response):
        # 1. Chọn tất cả các mục thông tin (các thẻ <li>)
        # Ta có thể dùng li.f-avenir-next-b hoặc ul > li đều được
        records = response.css('div.list-info-generals__lists li')
        
        # Regular Expression để tìm và tách (Ngày/Tháng/Năm) nằm trong ngoặc đơn
        # Ví dụ: (24/11/2025)
        date_pattern = re.compile(r'\s*\((?P<date>\d{2}/\d{2}/\d{4})\)$')

        for record in records:
            # Lấy toàn bộ nội dung text của thẻ <a> (Tiêu đề + Ngày)
            full_text_raw = record.css('a::text').get()
            
            # URL Bài viết
            article_url_relative = record.css('a::attr(href)').get()
            
            if full_text_raw:
                full_text = full_text_raw.strip()
                
                # Tìm kiếm chuỗi ngày tháng theo mẫu
                match = date_pattern.search(full_text)
                
                if match:
                    # Tách ngày tháng
                    date_string = match.group('date')
                    # Tách tiêu đề (loại bỏ chuỗi ngày tháng đã tìm thấy)
                    title = date_pattern.sub('', full_text).strip()
                else:
                    date_string = None
                    title = full_text
            else:
                title = None
                date_string = None

            e_item = EventItem()
            e_item['mcp'] = 'VCG'
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(article_url_relative)
            e_item['date'] = convert_date_to_iso8601(date_string)               
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
