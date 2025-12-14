import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_no1'
    mcpcty = 'NO1'
    # Thay thế bằng domain thực tế
    allowed_domains = ['911group.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://911group.com.vn/cong-bo-thong-tin'] 

    def parse(self, response):
        # 1. Lấy danh sách tất cả các thông báo (records)
        items = response.css('div.col-12.mb-4') 
        date_regex = r'(\d{2}/\d{2}/\d{4})'
        for item in items:
            # 1. Trích xuất Tiêu đề (Title)
            # Chọn thẻ <a> bên trong <h3> và lấy văn bản
            title = item.css('h3 a::text').get().strip() if item.css('h3 a::text').get() else None

            # 2. Trích xuất URL Chi tiết (Detail URL)
            # Chọn thẻ <a> bên trong <h3> và lấy thuộc tính href
            detail_url_relative = item.css('h3 a::attr(href)').get() 
            # Cần nối với domain gốc (nếu URL là tương đối)
            # Ví dụ: detail_url = response.urljoin(detail_url_relative)
            
            # 3. Trích xuất Tóm tắt/Nội dung (Excerpt)
            # Chọn thẻ <p> đứng ngay sau <h3>
            excerpt = item.css('div.news-content > p:not(.news-time)::text').get().strip() if item.css('div.news-content > p:not(.news-time)::text').get() else None
            
            # 1. Tách ngày tháng từ Excerpt
            published_date = None
            if excerpt:
                match = re.search(date_regex, excerpt)
                if match:
                    # Lấy nhóm kết quả đầu tiên (là chuỗi ngày tháng)
                    published_date = match.group(1)
            # 4. Trích xuất URL Hình ảnh đại diện (Image URL)
            # Chọn thẻ <img> bên trong div.home-news__img và lấy thuộc tính 'src'
            image_url = item.css('div.home-news__img img::attr(src)').get()
            # Cần nối với domain gốc (nếu URL là tương đối)

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(excerpt) +'\n' + str(detail_url_relative)
            e_item['date'] = convert_date_to_iso8601(published_date)               
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
