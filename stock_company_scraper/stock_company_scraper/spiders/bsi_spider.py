import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_bsi'
    mcpcty = 'BSI'
    # Thay thế bằng domain thực tế
    allowed_domains = ['bsc.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.bsc.com.vn/quan-he-co-dong/'] 

    def parse(self, response):
        # Lấy tất cả các item tin tức trong khối Công bố thông tin
        items = response.css('.news_service-item')

        for item in items:
            # 1. Trích xuất tiêu đề và nội dung
            title = (item.css('.main_title::text').get() or "").strip()
            summary = (item.css('.main_content::text').get() or "").strip()
            
            # 2. Lấy link PDF trực tiếp từ thuộc tính data-doccument
            pdf_link = item.css('::attr(data-doccument)').get()

            # 3. Xử lý ngày tháng (Ngày, Tháng, Năm nằm ở các thẻ khác nhau)
            month_text = item.css('.date::text').get() # Ví dụ: "Tháng 12"
            # Lấy text trực tiếp từ thẻ chứa ngày (loại bỏ phần span năm và month_text)
            day = item.xpath('.//p[contains(@class, "flex-1")]/text()').get()
            year = item.css('span.text-primary-300::text').get()

            # Làm sạch dữ liệu
            full_date = f"{day.strip() if day else ''} {month_text.strip() if month_text else ''}, {year.strip() if year else ''}"
            

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(summary)+'\n' + str(pdf_link)
            e_item['date'] = convert_vi_date_to_iso(full_date)               
            yield e_item

from datetime import datetime

def convert_vi_date_to_iso(date_str):
    # 1. Làm sạch chuỗi: bỏ dấu phẩy và chuyển về chữ thường
    date_str = date_str.replace(',', '').lower().strip()
    
    # 2. Ánh xạ tên tháng tiếng Việt sang số
    month_mapping = {
        'tháng 1': '01', 'tháng 2': '02', 'tháng 3': '03', 'tháng 4': '04',
        'tháng 5': '05', 'tháng 6': '06', 'tháng 7': '07', 'tháng 8': '08',
        'tháng 9': '09', 'tháng 10': '10', 'tháng 11': '11', 'tháng 12': '12'
    }
    
    # 3. Thay thế cụm từ "tháng X" bằng số tương ứng
    for vi_month, en_month in month_mapping.items():
        if vi_month in date_str:
            date_str = date_str.replace(vi_month, en_month)
            break
            
    # Hiện tại date_str có dạng: "25 01 2024"
    try:
        # 4. Parse chuỗi đã chuẩn hóa sang đối tượng datetime
        date_obj = datetime.strptime(date_str, "%d %m %Y")
        # 5. Trả về định dạng ISO 8601
        return date_obj.strftime("%Y-%m-%d")
    except ValueError:
        return None
