import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_ijc'
    mcpcty = 'IJC'
    # Thay thế bằng domain thực tế
    allowed_domains = ['becamexijc.com'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://becamexijc.com/quanhecodong/'] 

    def parse(self, response):
        items = response.css('.item-pdf')
        
        for item in items:
            # Trích xuất Ngày
            date_raw = item.css('.content time::text').get()
            
            # Trích xuất Tiêu đề
            title_text = item.css('.content h3 a::text').get().strip()
            
            # Trích xuất URL tệp PDF
            pdf_url = item.css('.content h3 a::attr(href)').get()
            
            # Định dạng lại ngày tháng
            published_date_iso = format_date(date_raw)

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title_text
            e_item['details_raw'] = str(title_text) +'\n' + str(pdf_url)
            e_item['date'] = published_date_iso               
            yield e_item

from datetime import datetime
import locale

def format_date(date_string):
        """Chuyển đổi ngày từ 'D tháng M, YYYY' sang 'YYYY-MM-DD'."""
        if not date_string:
            return None
        
        # Xử lý chuỗi "D tháng M, YYYY" (Ví dụ: "3 tháng 12, 2025")
        date_string = date_string.strip().replace('tháng', '').replace(',', '').strip()
        
        # Định dạng đầu vào: "%d %m %Y"
        # Lưu ý: %B cho tên tháng đầy đủ (tiếng Anh), nhưng ở đây ta dùng %m
        # Nếu Scrapy chạy trong môi trường có locale là Tiếng Việt, %B sẽ hoạt động.
        # Nhưng để an toàn, ta giả định %m nếu tháng chỉ là số/có thể xử lý được.
        # Vì đây là Tiếng Việt, chúng ta cần dùng locale hoặc thay thế thủ công.
        
        # Cách đơn giản hóa: Thay thế "tháng" bằng số (cần cấu hình locale hoặc ánh xạ).
        # Tạm thời ta sẽ sử dụng một cách tiếp cận robust hơn:
        
        date_parts = date_string.split()
        if len(date_parts) == 3:
            day = date_parts[0]
            month = date_parts[1]
            year = date_parts[2]
            
            # Ánh xạ tên tháng Tiếng Việt sang số (vì %m chỉ hiểu số)
            # Vì dữ liệu của bạn chỉ dùng số (1, 2, ... 12), ta chỉ cần ráp lại
            date_combined = f"{day.zfill(2)}/{month.zfill(2)}/{year}"
            
            try:
                # Định dạng đầu vào: DD/MM/YYYY
                date_object = datetime.strptime(date_combined, "%d/%m/%Y")
                return date_object.strftime("%Y-%m-%d")
            except ValueError:
                print(f"Không thể định dạng ngày (lỗi): {date_string}")
                return None
        
        return None
