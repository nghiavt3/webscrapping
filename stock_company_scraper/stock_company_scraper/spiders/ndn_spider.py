import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_ndn'
    mcpcty = 'NDN'
    # Thay thế bằng domain thực tế
    allowed_domains = ['ndn.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://ndn.com.vn/quan-he-co-dong'] 
    # def start_requests(self):
    #     yield scrapy.Request(
    #     url=self.start_urls[0],
    #     callback=self.parse,
    #     # Thêm meta để kích hoạt Playwright
    #     meta={"playwright": True,               
    #         }
    # )
    def parse(self, response):
        # Chọn tất cả các hàng trong thân bảng (tbody)
        rows = response.css('table.table tbody tr')

        for row in rows:
            # Trích xuất dữ liệu dựa trên thứ tự cột (nth-child)
            title = row.css('td:nth-child(2) a::text').get()
            symbol = row.css('td:nth-child(3)::text').get()
            raw_date = row.css('td:nth-child(4)::text').get()
            views = row.css('td:nth-child(5)::text').get()
            # Lấy link tải về từ cột cuối cùng
            download_url = row.css('td:nth-child(7) a::attr(href)').get()

            # Xử lý ngày tháng (DD/MM/YYYY -> YYYY-MM-DD)
            iso_date = None
            if raw_date:
                try:
                    date_obj = datetime.strptime(raw_date.strip(), '%d/%m/%Y')
                    iso_date = date_obj.strftime('%Y-%m-%d')
                except ValueError:
                    iso_date = raw_date.strip()

            if title:
                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = str(title.strip())
                e_item['details_raw'] = str(title.strip()) +'\n' + str(response.urljoin(download_url) if download_url else None)
                e_item['date'] = (iso_date)               
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
