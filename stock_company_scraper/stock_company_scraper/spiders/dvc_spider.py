import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_dvc'
    mcpcty= 'DVC'

    # Thay thế bằng domain thực tế
    allowed_domains = ['dichvucang.com'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.dichvucang.com/default.aspx?pageid=news&cate=3'] 

    def start_requests(self):
        yield scrapy.Request(
        url=self.start_urls[0],
        callback=self.parse,
        # Thêm meta để kích hoạt Playwright
        meta={'playwright': True}
    )
    
    def parse(self, response):
        # 1. Lặp qua từng mục tin (mỗi mục nằm trong một thẻ td)
        news_items = response.css('#Home1_ctl08_dlServices > tbody > tr > td')

        for item in news_items:
            
            # 2. Trích xuất Tiêu đề và URL
            title_anchor = item.css('.nd_center a')
            title = title_anchor.css('::text').get()
            url = title_anchor.css('::attr(href)').get()
            
            # 3. Trích xuất Ngày
            # Lấy toàn bộ nội dung text, sau đó làm sạch để lấy ngày
            date_raw = item.css('.noidung::text').get()
            
            # 4. Trích xuất URL ảnh
            image_url_relative = item.css('img.imgAnh::attr(src)').get()
            
            # Xử lý làm sạch và chuyển đổi URL tương đối sang tuyệt đối
            
            # Xử lý ngày: chỉ giữ lại phần ngày bên trong dấu ngoặc vuông và bỏ "Tin ngày"
            if date_raw:
                # Sử dụng slicing hoặc regex để lấy phần bên trong []
                start = date_raw.find('[')
                end = date_raw.find(']')
                pub_date = date_raw[start+1:end] if start != -1 and end != -1 else date_raw.replace('Tin ngày', '').strip()
            else:
                pub_date = None
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
    input_format = '%d/%m/%Y %H:%M'
    
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
