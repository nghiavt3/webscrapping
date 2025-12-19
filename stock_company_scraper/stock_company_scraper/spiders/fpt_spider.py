import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_fpt'
    mcpcty = 'FPT'
    # Thay thế bằng domain thực tế
    allowed_domains = ['fpt.com'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://fpt.com/vi/nha-dau-tu/thong-tin-cong-bo'] 

    # def start_requests(self):
    #     yield scrapy.Request(
    #     url=self.start_urls[0],
    #     callback=self.parse,
    #     # Thêm meta để kích hoạt Playwright
    #     meta={'playwright': True}
    # )
        
    def parse(self, response):
        # 1. Lặp qua từng khối tháng (mỗi khối là một 'component')
        months = response.css('div.media-download-section-key-information')
        
        for month_block in months:
            # Lấy tên tháng (ví dụ: "Tháng 12/2025")
            month_label = month_block.css('.media-download-section-key-information-title::text').get()
            if month_label:
                month_label = month_label.strip()

            # 2. Lặp qua từng tin tức bên trong khối tháng đó
            # Mỗi tin tức nằm trong div 'media-download-section-key-information-content'
            items = month_block.css('.media-download-section-key-information-content')
            
            for item in items:
                # Trích xuất tiêu đề
                title = item.css('a.media-download-section-key-information-content-subtitle::text').get()
                
                # Trích xuất link (nối với domain nếu cần)
                link = item.css('a.media-download-section-key-information-content-subtitle::attr(href)').get()
                
                # Trích xuất ngày cập nhật (nằm ở div ngay sau item hiện tại hoặc trong icon-date)
                date_raw = item.css('.media-download-section-key-information-description-date::text').get()
                clean_date = date_raw.replace('Cập nhật:', '').strip()
                # Xử lý làm sạch dữ liệu

                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = title.strip()
                e_item['details_raw'] = str(title.strip()) +'\n' + str(response.urljoin(link))
                e_item['date'] = convert_date_to_iso8601(clean_date)
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
