import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_pc1'
    mcpcty = 'PC1'
    # Thay thế bằng domain thực tế
    allowed_domains = ['pc1group.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.pc1group.vn/category/quan-he-dau-tu/cong-bo-thong-tin/'] 

    def parse(self, response):
        items = response.css('.vc_grid-item.vc_clearfix.vc_col-sm-6.vc_visible-item')
        data = []

        for item in items:
            # 1. Trích xuất Tiêu đề và URL
            # Tiêu đề và URL nằm trong thẻ <a> trong <h4>
            title_selector = 'h4 a::text'
            url_selector = 'h4 a::attr(href)'
            
            title = item.css(title_selector).get()
            url = item.css(url_selector).get()
            
            # 2. Trích xuất Ngày
            # Ngày nằm trong <div> thứ 2 với class vc_gitem-post-data-source-post_date
            date_selector = '.vc_custom_heading.vc_gitem-post-data.vc_gitem-post-data-source-post_date div::text'
            date = item.css(date_selector).get()
            
            # 3. Trích xuất URL hình ảnh (tùy chọn)
            # URL hình ảnh nằm trong thuộc tính 'src' của thẻ <img> trong thẻ <a> đầu tiên
            image_url_selector = 'figure a img::attr(src)'
            image_url = item.css(image_url_selector).get()

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = title +'\n' + url
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
