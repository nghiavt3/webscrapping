import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_apg'
    mcpcty = 'APG'
    # Thay thế bằng domain thực tế
    allowed_domains = ['apsi.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://apsi.vn/investors/info_fin','https://apsi.vn/investors/share_holders','https://apsi.vn/investors/info_disc'] 

    # def start_requests(self):
    #     yield scrapy.Request(
    #     url=self.start_urls[0],
    #     callback=self.parse,
    #     # Thêm meta để kích hoạt Playwright
    #     meta={'playwright': True}
    # )
        
    def parse(self, response):
        for item in response.css('div.item--invest'):
            # 1. Trích xuất text cơ bản
            title = (item.css('.content .title::text').get() or "").strip()
            publish_date = (item.css('.content .publishDate::text').get() or "").strip()
            year = item.css('.year::text').get()

            # 2. Xử lý link từ thuộc tính phx-click
            # Giá trị mẫu: [["navigate",{"replace":false,"href":"/investors-details?id=..."}]]
            phx_data = item.attrib.get('phx-click', '')
            link = None
            
            if phx_data:
                # Dùng regex để tìm đoạn href
                match = re.search(r'"href":"([^"]+)"', phx_data)
                if match:
                    relative_url = match.group(1)
                    link = response.urljoin(relative_url)

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(link)
            e_item['date'] = (publish_date)               
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
