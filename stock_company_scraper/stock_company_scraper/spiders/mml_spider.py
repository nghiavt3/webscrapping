import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_mml'
    mcpcty = 'MML'
    # Thay thế bằng domain thực tế
    allowed_domains = ['masanmeatlife.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://masanmeatlife.com.vn/quan-he-co-dong/thong-bao-cong-ty/tat-ca'] 

    # def start_requests(self):
    #     yield scrapy.Request(
    #     url=self.start_urls[0],
    #     callback=self.parse,
    #     # Thêm meta để kích hoạt Playwright
    #     meta={'playwright': True}
    # )
        
    def parse(self, response):
        for item in response.css('div.item-flex'):
            # Trích xuất tiêu đề (text nằm trong thẻ p class name report-name)
            title = item.css('p.report-name::text').get(default='').strip(),
            
            # Trích xuất ngày (thẻ p thứ nhất trong div.dated)
            date = item.css('div.dated p:nth-of-type(1)::text').get(),
            
            # Trích xuất giờ (thẻ p thứ hai trong div.dated)
            time = item.css('div.dated p:nth-of-type(2)::text').get(),
            
            # Trích xuất link tải xuống (thuộc tính href của thẻ a trong div.taixuong)
            download_link = response.urljoin(item.css('div.taixuong a::attr(href)').get()),
            
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = str(title)
            e_item['details_raw'] = str(title) +'\n' + str(download_link)
            e_item['date'] = convert_date_to_iso8601(date[0])
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
