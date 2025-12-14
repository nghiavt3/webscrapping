import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_pvx'
    # Thay thế bằng domain thực tế
    allowed_domains = ['petrocons.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://petrocons.vn/quan-he-co-dong'] 

    def parse(self, response):
        # 1. Lấy danh sách tất cả các thông báo (records)
        # 1. Chọn tất cả các khối tin tức riêng lẻ
        posts = response.css('div.col-lg-6.p-b-50')
        
        for post in posts:
            # 2. Trích xuất Tiêu đề và URL từ thẻ <h4>
            title_url_element = post.css('h4.p-b-12 a')
            
            title = title_url_element.css('::text').get().strip()
            url = title_url_element.css('::attr(href)').get()
            
            # 3. Trích xuất Ngày công bố
            # Dùng selector nâng cao để lấy span chứa ngày
            date = post.css('i.fa-calendar + span::text').get()
            if date:
                date = date.strip()
            
            # 4. Trích xuất Tác giả
            # Dùng selector nâng cao để lấy thẻ <a> chứa tác giả
            author = post.css('i.fa-user + a::text').get()
            if author:
                author = author.strip()

            e_item = EventItem()
            e_item['mcp'] = 'PVX'
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(url)
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
