import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_cms'
    mcpcty = 'CMS'
    # Thay thế bằng domain thực tế
    allowed_domains = ['cmhgroup.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://cmhgroup.vn/danh-muc/cong-bo-thong-tin/'] 

    def parse(self, response):
        # 1. Lấy danh sách tất cả các thông báo (records)
        items = response.css('div.row.row-small.thong-tin-co-dong-row-item') 

        for item in items:
            # 1. Trích xuất Tiêu đề (Title)
            # Chọn thẻ <h5> có class 'title'
            title = item.css('h5.title::text').get().strip() if item.css('h5.title::text').get() else None

            # 2. Trích xuất Ngày công bố (Date)
            # Chọn thẻ <p> có class 'entry-date'
            date = item.css('p.entry-date::text').get().strip() if item.css('p.entry-date::text').get() else None

            # 3. Trích xuất URL file (URL)
            # URL này được tìm thấy ở hai nơi:
            # a) Thuộc tính href của thẻ <a> bao quanh tiêu đề
            # b) Thuộc tính href của thẻ <a> download
            file_url = item.css('div.col.medium-7 a::attr(href)').get()

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(file_url)
            e_item['date'] = convert_date_to_iso8601(date)               
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
