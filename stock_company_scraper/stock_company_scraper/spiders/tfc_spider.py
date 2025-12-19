import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
import json
class EventSpider(scrapy.Spider):
    name = 'event_tfc'
    mcpcty = 'TFC'
    # Thay thế bằng domain thực tế
    allowed_domains = ['gateway.fpts.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://gateway.fpts.com.vn/news/api/gateway/v1/mobile/list?folder=86&code=TFC&pageSize=8&selectedPage=1&cbtt=1&from=01-01-1970&to=01-01-3000&newsType=1'] 
    
    def start_requests(self):
        """Gửi request đến API với header thích hợp."""
        for url in self.start_urls:
            # Tùy chọn: Đặt header để mô phỏng một request từ trình duyệt/ứng dụng
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                headers={
                    'Accept': 'application/json',
                    # Thêm User-Agent để tránh bị chặn
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
            )

    def parse(self, response):
        """Xử lý response JSON."""
        
        # 1. Tải dữ liệu JSON
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error("Không thể decode JSON từ response!")
            return

        # 2. Kiểm tra mã lỗi và truy cập mảng dữ liệu
        if data.get("Code") != 0 or "Table1" not in data.get("Data", {}):
            self.logger.error(f"API trả về lỗi hoặc không có dữ liệu: {data.get('Message')}")
            return
            
        news_items = data["Data"]["Table1"]

        # 3. Trích xuất từng trường dữ liệu
        for item in news_items:
            # Lấy phần "attributes" - đây là bước quan trọng để truy cập các trường
            title = item.get('Title')
            pub_date = item.get('DatePub')
            url = item.get('URL')

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) + '\n' + str(url)
            e_item['date'] = convert_date_to_iso8601(pub_date)
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
    input_format = '%d/%m/%Y %H:%M'
                    
    # Định dạng đầu ra: Năm-Tháng-Ngày ('%Y-%m-%d') - chuẩn ISO 8601 cho ngày
    output_format = '%Y-%m-%d'

    try:
        # 1. Parse chuỗi đầu vào thành đối tượng datetime
        date_object = datetime.strptime(vietnam_date_str.split('.')[0], input_format)
        
        # 2. Định dạng lại đối tượng datetime thành chuỗi ISO 8601
        iso_date_str = date_object.strftime(output_format)
        
        return iso_date_str
    
    except ValueError as e:
        print(f"⚠️ Lỗi chuyển đổi ngày tháng '{vietnam_date_str}' (phải là DD/MM/YYYY): {e}")
        return None
