import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_htg'
    mcpcty= 'HTG'
    # Lấy đối tượng ngày giờ hiện tại
    current_datetime = datetime.now()

    # Trích xuất thuộc tính năm (year)
    current_year = current_datetime.year

    # Thay thế bằng domain thực tế
    allowed_domains = ['hoatho.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://hoatho.com.vn/quan-he-co-dong/thong-tin-co-dong/'+str(current_year)] 

    
    def parse(self, response):
        # Selector chính: Chọn tất cả các khối tài liệu PDF
        document_items = response.css('div.item-pdf')
        
        for item_selector in document_items:
            
            # 1. Trích xuất Ngày Đăng
            date_raw = item_selector.css('div.date::text').get()
            if date_raw:
                pub_date = date_raw.strip()
            
            # 2. Trích xuất Tiêu đề và URL từ thẻ <a> trong div.title
            link_selector = item_selector.css('div.title a')
            
            # Tiêu đề
            title_raw = link_selector.css('::text').get()
            if title_raw:
                title = title_raw.strip()
            
            # Link Tải về (là thuộc tính 'href' của thẻ <a>)
            relative_url = link_selector.css('::attr(href)').get()
            if relative_url:
                # response.urljoin dùng để chuyển URL tương đối thành URL tuyệt đối
                url = response.urljoin(relative_url)

            e_item = EventItem()
            # 2. Trích xuất dữ liệu chi tiết
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['date'] = convert_date_to_iso8601(pub_date)
            e_item['summary'] = title
            
            e_item['details_raw'] = str(title) + '\n' + str(url) 
                         
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
