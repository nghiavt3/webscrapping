import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_ors'
    mcpcty = 'ORS'
    # Thay thế bằng domain thực tế
    allowed_domains = ['tpbs.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://tpbs.com.vn/vi/thong-tin-tps/quan-he-co-dong/cong-bo-thong-tin','https://tpbs.com.vn/vi/thong-tin-tps/quan-he-co-dong/thong-tin-co-dong'] 

    def parse(self, response):
        # Lặp qua từng hàng của bảng
        for row in response.css('tr.itemRow'):
            # 1. Trích xuất tiêu đề (nằm trong thẻ a)
            title = row.css('td a::text').get()
            
            # 2. Trích xuất link và chuyển thành link tuyệt đối
            link = row.css('td a::attr(href)').get()
            
            # 3. Trích xuất ngày tháng (nằm trong thẻ i)
            # Định dạng gốc trong HTML đã là YYYY-MM-DD (ví dụ: 2025-12-05)
            date_iso = row.css('td i::text').get()

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(link)
            e_item['date'] = (date_iso)               
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
