import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_tta'
    mcpcty = 'TTA'
    # Thay thế bằng domain thực tế
    allowed_domains = ['truongthanhgroup.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://truongthanhgroup.com.vn/co-dong/cong-bo-thong-tin/'] 

    def parse(self, response):
        # 1. Định vị bảng chính xác bằng ID
        # Sử dụng CSS Selector để chọn thẻ <tbody> của bảng có ID là 'event-formdata'
        rows = response.css('tr.item-shareholder')
        
        for row in rows:
            
            
            # STT: cột thứ nhất
            stt = row.css('td:nth-child(1) span::text').get().strip()
            
            # Ngày Đăng: cột thứ hai
            pub_date = row.css('td:nth-child(2) span::text').get().strip()
            
            # Nội dung: cột thứ ba, bên trong thẻ div có class 'title-shareholder'
            # Sử dụng .get().strip() để lấy văn bản và loại bỏ khoảng trắng thừa
            title_raw = row.css('td:nth-child(3) div.title-shareholder::text').get()
            if title_raw:
                title = title_raw.strip()
            else:
                title = None # Xử lý trường hợp không có nội dung
                
            # Link Tải về: cột thứ tư, lấy thuộc tính 'href' của thẻ <a>
            url = row.css('td:nth-child(4) a::attr(href)').get()
            
            # --- 4. Làm sạch và Gán vào Item ---
            item  = EventItem()
            item['mcp'] = self.mcpcty
            item['web_source'] = self.allowed_domains[0]
            item['summary'] = title.strip()
            item['date'] = convert_date_to_iso8601(pub_date)
            item['details_raw'] = str(title.strip()) + '\n' + str(url)
            #item['details_clean'] = download_url
            #item['download_url'] = download_url
            yield item

    

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
