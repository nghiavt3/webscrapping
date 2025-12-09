import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
import json
class EventSpider(scrapy.Spider):
    name = 'event_idc'
    mcpcty = 'IDC'
    # Thay thế bằng domain thực tế
    allowed_domains = ['admin.idico.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://admin.idico.com.vn/api/tai-lieus?populate=files.media&filters[category][$eq]=C%C3%B4ng%20b%E1%BB%91%20th%C3%B4ng%20tin&filters[files][title][$containsi]=&locale=vi'] 
    
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
        
        # # 1. Tải dữ liệu JSON
        # try:
        #     data = json.loads(response.text)
        # except json.JSONDecodeError:
        #     self.logger.error("Không thể decode JSON từ response!")
        #     return
        data = response.json().get('data', [])
        
            
        #news_items = data["data"]

        # 3. Trích xuất từng trường dữ liệu
        for item in data:
            # Lấy phần "attributes" - đây là bước quan trọng để truy cập các trường
            attributes = item.get('attributes', {})
            files = attributes.get('files',[])
            for file in files:
                title = file.get('title')
                date = file.get('override_date')
                pdf_path = file.get('media', {}).get('data', {}).get('attributes', {}).get('url')
                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = title
                e_item['details_raw'] = str(title) + '\n' +'https://admin.idico.com.vn'+ str(pdf_path)
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
    input_format = '%Y-%m-%dT%H:%M:%S'
    
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
