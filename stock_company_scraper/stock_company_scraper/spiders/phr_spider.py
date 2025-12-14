import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_phr'
    mcpcty = 'PHR'
    # Thay thế bằng domain thực tế
    allowed_domains = ['phr.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.phr.vn/thong-tin-co-dong.aspx?catid=6'] 

    def start_requests(self):
        yield scrapy.Request(
        url=self.start_urls[0],
        callback=self.parse,
        # Thêm meta để kích hoạt Playwright
        meta={'playwright': True}
    )
    def parse(self, response):
        news_blocks = response.css('div.news-stock-list > div[class^="bc-list"]')
        
        for block in news_blocks:
            # 2. Trích xuất Tiêu đề và URL chi tiết
            
            # Selector cho Tiêu đề và URL chi tiết nằm ngay dưới block
            detail_link_tag = block.css('a:first-child')
            
            tieu_de = detail_link_tag.css('::text').get().strip()
            # Lấy URL chi tiết (dạng tương đối)
            detail_url_relative = detail_link_tag.css('::attr(href)').get()
            
            # 3. Trích xuất URL Tải về và URL Xem (PDF)
            
            # Khối chứa các liên kết tải về/xem
            download_container = block.css('div.bc-list-down-in')
            
            # URL Xem (thẻ <a> đầu tiên trong bc-list-down-in)
            view_url = download_container.css('a:nth-child(1)::attr(href)').get()
            
            # URL Tải về (thẻ <a> thứ hai trong bc-list-down-in)
            download_url = download_container.css('span a:nth-child(1)::attr(href)').get()
            
            # Lưu ý: Trong trường hợp này, view_url và download_url là giống nhau.
            # Ta vẫn trích xuất cả hai để đảm bảo độ chính xác theo cấu trúc.

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = tieu_de
            e_item['details_raw'] = str(tieu_de) +'\n' + str(view_url) +'\n' + str(download_url)
            e_item['date'] = None
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
