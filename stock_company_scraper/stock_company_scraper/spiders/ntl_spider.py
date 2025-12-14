import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_ntl'
    mcpcty = 'NTL'
    # Thay thế bằng domain thực tế
    allowed_domains = ['lideco.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://lideco.vn/chuyen-muc/quan-he-co-dong/'] 

    def parse(self, response):
        posts = response.css('article.ecs-post-loop')
        
        for post in posts:
            # Dữ liệu nằm trong khối .elementor-section-wrap bên trong mỗi article
            # 2. Trích xuất Tiêu đề và URL chi tiết
            # Selector: tìm thẻ a nằm trong .elementor-heading-title
            title_selector = post.css('.elementor-heading-title a')
            
            title = title_selector.css('::text').get()
            detail_url = title_selector.css('::attr(href)').get() 

            # 3. Trích xuất Ngày công bố
            # Selector: tìm li có itemprop="datePublished", lấy text từ thẻ span bên trong
            date_published = post.css('li[itemprop="datePublished"] span.elementor-post-info__item--type-date::text').get()
            
            # 4. Trích xuất URL Tải xuống (PDF Link)
            # Selector: tìm thẻ a nằm trong .elementor-image, lấy thuộc tính href
            # Lưu ý: Một số bài có thể không có link tải xuống (như post-8531, post-8461, post-8310 trong HTML)
            download_url = post.css('.elementor-image a::attr(href)').get()
            title_str = title if title else ''
            detail_url_str = detail_url if detail_url else ''
            download_url_str = download_url if download_url else ''

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title_str) + '\n' + str(detail_url_str) + '\n' + str(download_url_str)
            e_item['date'] = convert_date_to_iso8601(date_published)
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
    input_format = "%d/%m/%Y"    
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
