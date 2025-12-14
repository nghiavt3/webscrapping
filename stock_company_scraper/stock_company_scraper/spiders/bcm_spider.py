import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
import locale
class EventSpider(scrapy.Spider):
    name = 'event_bcm'
    mcpcty = 'BCM'
    # Thay thế bằng domain thực tế
    allowed_domains = ['becamex.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://becamex.com.vn/quan-he-co-dong/cong-bo-thong-tin/'] 

    def normalize_date(self, date_str):
        """Chuyển đổi ngày tháng tiếng Việt sang định dạng ISO 8601 (YYYY-MM-DD)"""
        try:
            # Định dạng đầu vào: '02 Tháng 12, 2025' (sử dụng %B cho tên tháng đầy đủ theo locale)
            datetime_obj = datetime.strptime(date_str, '%d Tháng %B, %Y')
            return datetime_obj.strftime('%Y-%m-%d')
        except Exception as e:
            # Nếu việc định dạng locale thất bại, trả về chuỗi gốc
            self.logger.warning(f"Không thể chuẩn hóa ngày '{date_str}': {e}")
            return date_str
        
    def parse(self, response):
        # Thiết lập locale để xử lý định dạng ngày tháng tiếng Việt ('02 Tháng 12, 2025')
        # Lưu ý: Việc thiết lập locale có thể không hoạt động trên mọi hệ thống/môi trường.
        try:
            locale.setlocale(locale.LC_TIME, 'vi_VN.UTF-8')
        except locale.Error:
            try:
                locale.setlocale(locale.LC_TIME, 'vi_VN')
            except locale.Error:
                # Fallback: có thể cần xử lý thủ công hoặc bỏ qua việc chuẩn hóa ngày
                pass

        # 1. Chọn tất cả các khối tin tức
        news_blocks = response.css('div#shareholder-list .shareholder-item')
        
        for block in news_blocks:
            # 2. Trích xuất Ngày công bố
            date_raw = block.css('p.text-primary-1::text').get().strip()
            
            # 3. Trích xuất Tiêu đề và URL tài liệu (PDF)
            title_link_tag = block.css('h2 a')
            tieu_de = title_link_tag.css('::text').get().strip()
            pdf_url = title_link_tag.css('::attr(href)').get()
            
            # Chuẩn hóa ngày (chuyển đổi sang YYYY-MM-DD)
            date_iso = self.normalize_date(date_raw)
            

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = tieu_de
            e_item['details_raw'] = str(tieu_de) + '\n' + str(pdf_url)
            e_item['date'] = normalize_vietnamese_date_ultimate(date_raw)
            yield e_item

from datetime import datetime

def normalize_vietnamese_date_ultimate(date_str):
    """
    Chuyển đổi chuỗi ngày tháng tiếng Việt sang ISO 8601, xử lý triệt để
    các ký tự khoảng trắng không chuẩn (\xa0) và khoảng trắng thừa.
    """
    
    # Bản đồ thay thế tên tháng tiếng Việt bằng số (Key: Tên tháng, Value: Số tháng)
    month_mapping = {
        'Tháng 1': '01', 'Tháng 2': '02', 'Tháng 3': '03', 'Tháng 4': '04', 
        'Tháng 5': '05', 'Tháng 6': '06', 'Tháng 7': '07', 'Tháng 8': '08', 
        'Tháng 9': '09', 'Tháng 10': '10', 'Tháng 11': '11', 'Tháng 12': '12'
    }
    
    # 1. Làm sạch triệt để:
    #   - Loại bỏ khoảng trắng thừa ở đầu/cuối bằng strip()
    #   - Thay thế các ký tự khoảng trắng không ngắt (hoặc bất kỳ ký tự nào không phải chữ/số) bằng khoảng trắng
    temp_date_str = date_str.strip().replace('\xa0', ' ')
    
    # 2. Chuẩn hóa khoảng trắng và định dạng: 
    #    - Tách chuỗi bằng khoảng trắng, sau đó nối lại bằng một khoảng trắng duy nhất.
    #    - Thay thế dấu phẩy bằng khoảng trắng để đơn giản hóa định dạng.
    parts = temp_date_str.replace(',', '').split()
    
    # Ví dụ: ['02', 'Tháng', '12', '2025']
    
    if len(parts) != 4:
        print(f"Lỗi: Chuỗi không đúng định dạng 4 phần: {date_str}")
        return date_str

    day, month_name, month_num_str, year = parts[0], parts[1], parts[2], parts[3]
    
    # Ghép lại tên tháng để tìm trong mapping (ví dụ: 'Tháng 12')
    full_month_name = f"{month_name} {month_num_str}"
    
    if full_month_name in month_mapping:
        # Thay thế tên tháng tiếng Việt bằng số tháng
        month_iso = month_mapping[full_month_name]
        
        # 3. Chuỗi cuối cùng để parse: 'DD/MM/YYYY'
        final_date_str = f"{day}/{month_iso}/{year}"
        format_in = '%d/%m/%Y'
        
        try:
            # 4. Chuyển đổi sang đối tượng datetime và định dạng ISO 8601
            datetime_obj = datetime.strptime(final_date_str, format_in)
            return datetime_obj.strftime('%Y-%m-%d')
            
        except ValueError as e:
            print(f"Lỗi cú pháp cuối cùng: Chuỗi='{final_date_str}', Lỗi: {e}")
            return date_str
    else:
        print(f"Lỗi: Không tìm thấy tên tháng '{full_month_name}' trong mapping.")
        return date_str