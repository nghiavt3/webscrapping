import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_tig'
    # Thay thế bằng domain thực tế
    allowed_domains = ['tig.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://tig.vn/vi/co-dong/cong-bo-thong-tin-3120/page-1.spp'] 

    def parse(self, response):
        # 1. Lấy danh sách tất cả các thông báo (records)
        documents = response.css('.right .item')
    
        for doc in documents:
            date_raw = doc.css('.date::text').get()
            title_raw = doc.css('.title::text').get()
        
            # URL tải tệp: Lấy từ thẻ <a> trong .attach-file
            # Lưu ý: Scrapy sẽ tự động xử lý URL tương đối (relative URL) 
            # khi bạn sử dụng response.urljoin() nếu cần
            download_url_relative = doc.css('.attach-file a::attr(href)').get()
        
            # Các tệp đính kèm khác (từ thẻ <select>)
            file_options = []
            for option in doc.css('.attach-file select option'):
                file_options.append({
                'name': option.css('::text').get(),
                'download_link': option.css('::attr(value)').get(),
            })
            
            
            e_item = EventItem()
            e_item['mcp'] = 'tig'
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title_raw
            e_item['details_raw'] = title_raw +'\n' + download_url_relative
            e_item['date'] = convert_date_to_iso8601(date_raw)               
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
