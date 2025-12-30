import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_ocb'
    mcpcty= 'OCB'

    # Thay thế bằng domain thực tế
    allowed_domains = ['ocb.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://ocb.com.vn/vi/nha-dau-tu'] 

    # def start_requests(self):
    #     yield scrapy.Request(
    #     url=self.start_urls[0],
    #     callback=self.parse,
    #     # Thêm meta để kích hoạt Playwright
    #     meta={'playwright': True}
    # )
    
    def parse(self, response):
        # Chọn tất cả các mục tin tức
        items = response.css('div.content__info-item')

        for item in items:
            # Trích xuất tiêu đề (text bên trong thẻ a)
            title = item.css('a::text').get()
            
            # Trích xuất đường dẫn file (thuộc tính href của thẻ a)
            link = item.css('a::attr(href)').get()
            
            # Trích xuất ngày đăng (xử lý bỏ chữ "Ngày đăng: ")
            raw_date = item.css('div.published-date::text').get()
            clean_date = raw_date.replace('Ngày đăng: ', '').strip() if raw_date else None
            e_item = EventItem()
            # 2. Trích xuất dữ liệu chi tiết
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['date'] = convert_date_to_iso8601(clean_date)
            e_item['summary'] = title           
            e_item['details_raw'] = str(title) +'\n' + str(link)
                        
            yield e_item

        # 1. Lặp qua từng nhóm danh mục (Tài liệu Đại hội, Thông báo...)
        groups = response.css('div.content__item')

        for group in groups:
            # Trích xuất tên danh mục (ví dụ: "Thông báo")
            category = group.css('.content__title span::text').get()
            
            # 2. Lặp qua từng bản tin trong danh mục đó
            news_items = group.css('.content__info-item')
            for item in news_items:
                # Trích xuất tiêu đề và link
                title = item.css('a::text').get()
                link = item.css('a::attr(href)').get()
                
                # Trích xuất và làm sạch ngày đăng
                raw_date = item.css('.published-date::text').get()
                clean_date = raw_date.replace('Ngày đăng: ', '').strip() if raw_date else None

                e_item = EventItem()
                # 2. Trích xuất dữ liệu chi tiết
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['date'] = convert_date_to_iso8601(clean_date)
                e_item['summary'] = title           
                e_item['details_raw'] = str(title) +'\n' + str(link)
                        
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
