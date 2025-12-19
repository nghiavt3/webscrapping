import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_tis'
    mcpcty = 'TIS'
    # Thay thế bằng domain thực tế
    allowed_domains = ['tisco.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://tisco.com.vn/quan-he-co-dong/thong-bao.html'] 

    # def start_requests(self):
    #     yield scrapy.Request(
    #     url=self.start_urls[0],
    #     callback=self.parse,
    #     # Thêm meta để kích hoạt Playwright
    #     meta={'playwright': True}
    # )
        
    def parse(self, response):
        # Chọn bảng theo ID và duyệt qua từng dòng trong tbody
        first_news = response.css('.first-news')
        if first_news:
            title = first_news.css('.title a::text').get()
            date = first_news.css('.date::text').get()
            url = response.urljoin(first_news.css('.title a::attr(href)').get())
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = str(title)
            e_item['details_raw'] = str(title) +'\n' + str(url)
            e_item['date'] = convert_date_to_iso8601(date)
            yield e_item
        
        for item in response.css('.items-bottom li'):
            # Xử lý làm sạch chuỗi ngày (bỏ chữ "Cập nhật ")
            raw_date = item.css('.date::text').get()
            clean_date = raw_date.replace('Cập nhật ', '').strip() if raw_date else None
            title = item.css('.title a::text').get()
            url = response.urljoin(item.css('.title a::attr(href)').get())
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = str(title)
            e_item['details_raw'] = str(title) +'\n' + str(url)
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
