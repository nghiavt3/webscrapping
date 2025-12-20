import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_psi'
    mcpcty = 'PSI'
    # Thay thế bằng domain thực tế
    allowed_domains = ['psi.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.psi.vn/vi/quan-he-nha-dau-tu/cong-bo-thong-tin'] 

    def parse(self, response):
        # Duyệt qua từng hàng dữ liệu
        for item in response.css('.article-cell'):
            # Trích xuất ngày và tháng/năm
            day = item.css('.date-tag__date::text').get()
            month_year = item.css('.date-tag__month-year::text').get()
            
            # Trích xuất tiêu đề và link
            title = item.css('a.text--bold::text').get()
            download_link = item.css('a.text--bold::attr(href)').get()
            date= f"{day.strip()}/{month_year.strip()}" if day and month_year else None,
            link= response.urljoin(download_link) if download_link else None,

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(link)
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
