import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_vnd'
    mcpcty = 'VND'
    # Thay thế bằng domain thực tế
    allowed_domains = ['vndirect.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.vndirect.com.vn/danh_muc_quan_he_co_dong/cong-bo-thong-tin/'] 

    def parse(self, response):
        items = response.css('div.news-item')

        for item in items:
            # 1. Trích xuất ngày tháng (Ghép Ngày/Tháng và Năm)
            day_month = item.css('span.date-day::text').get() or ""
            month_part = item.css('sup::text').get() or "" # Chứa chuỗi " /12"
            year = item.css('p.date-year::text').get() or ""
            full_date = f"{day_month.strip()}{month_part.strip()}/{year.strip()}"

            # 2. Trích xuất tiêu đề
            title = (item.css('h3 a::text').get() or "").strip()

            # 3. Trích xuất danh sách file đính kèm (Lấy cả tên và link)
            files = []
            file_elements = item.css('ul.listd li a')
            for f in file_elements:
                files.append({
                    'file_name': (f.css('::text').get() or "").strip(),
                    'file_url': f.css('::attr(href)').get()
                })
            
            # Trường hợp tin tức chỉ có 1 file trực tiếp (không nằm trong danh sách ul)
            if not files:
                single_file_url = item.css('h3 a::attr(href)').get()
                if single_file_url and single_file_url.endswith('.pdf'):
                    files.append({
                        'file_name': title,
                        'file_url': single_file_url
                    })

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(files)
            e_item['date'] = convert_date_to_iso8601(full_date)               
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
