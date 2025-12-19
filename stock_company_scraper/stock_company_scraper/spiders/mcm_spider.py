import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_mcm'
    mcpcty = 'MCM'
    # Thay thế bằng domain thực tế
    allowed_domains = ['mcmilk.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.mcmilk.com.vn/quan-he-co-dong/cong-bo-thong-tin-khac/'] 

    # def start_requests(self):
    #     yield scrapy.Request(
    #     url=self.start_urls[0],
    #     callback=self.parse,
    #     # Thêm meta để kích hoạt Playwright
    #     meta={'playwright': True}
    # )
        
    def parse(self, response):
        # Chọn tất cả các thẻ article đại diện cho mỗi bài đăng
        articles = response.css('article.post-entry')

        for article in articles:
            # 1. Trích xuất tiêu đề
            title = article.css('h2.post-title a::text').get()
            
            # 2. Trích xuất ngày tháng chuẩn ISO (YYYY-MM-DD) 
            # Dùng class .av-structured-data ẩn bên dưới để lấy dữ liệu sạch nhất
            iso_date_full = article.css('span.av-structured-data[itemprop="datePublished"]::text').get()
            # Cắt chuỗi lấy 10 ký tự đầu: "2025-12-05"
            iso_date = iso_date_full[:10] if iso_date_full else None
            
            # 3. Trích xuất link PDF trực tiếp
            # Tìm thẻ <a> trong div entry-content có chứa chữ "Xem chi tiết"
            pdf_link = article.css('div.entry-content p a::attr(href)').get()
            
            # 4. Trích xuất link bài viết (nếu cần dự phòng)
            post_link = article.css('h2.post-title a::attr(href)').get()
            
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title.strip()
            e_item['details_raw'] = str(title.strip()) +'\n' + str(post_link)
            e_item['date'] = (iso_date)
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
