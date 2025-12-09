import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re


class EventSpider(scrapy.Spider):
    name = 'event_tpb'
    allowed_domains = ['tpb.vn']
    start_urls = ['https://tpb.vn/nha-dau-tu/thong-bao-co-dong']

    # Lấy thông tin ngày tháng hiện tại
    now = datetime.now()
    # current_month: 12
    current_month_int = now.month
    # current_year: 2025
    current_year_str = str(now.year)
    
    # Ghi đè cấu hình CHỈ CHO SPIDER NÀY
    custom_settings = {
        # Đã tăng timeout trong settings.py để tránh lỗi lần trước (giả định)
        'CONCURRENT_REQUESTS': 1,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
    }

    def start_requests(self):
        # Sử dụng PlaywrightRequest để đảm bảo trang được tải đầy đủ
        yield scrapy.Request(
            url='https://tpb.vn/nha-dau-tu/thong-bao-co-dong',
            callback=self.parse,
            # Playwright middleware sẽ tự động chuyển đổi yêu cầu này
            meta={'playwright': True} 
        )
        
    def parse(self, response):
        # CHỌN TẤT CẢ CÁC KHỐI NỘI DUNG (GROUP-CONTENT)
        groups = response.css('div.group-content')
        
        # Chúng ta chỉ cần kiểm tra nhóm đầu tiên (vì nó thường là tháng mới nhất)
        # Tuy nhiên, vòng lặp này sẽ kiểm tra tất cả các nhóm để tìm tháng hiện tại
        for group in groups:
            # Trích xuất thông tin Tháng và Năm từ khối hiện tại
            month_raw = group.css('.b_left .month-value::text').get()
            year_raw = group.css('.b_left .year-value::text').get()

            # Nếu không tìm thấy thông tin tháng/năm, bỏ qua
            if not month_raw or not year_raw:
                continue

            # Xử lý chuỗi
            month_str = month_raw.strip().replace('T', '').replace('háng', '') # Thường trả về "12"
            year_str = year_raw.strip() # Thường trả về "2025"

            # Kiểm tra xem khối này có phải là Tháng/Năm hiện tại không
            try:
                # Chuyển tháng về dạng số nguyên để so sánh
                month_int = int(month_str)
            except ValueError:
                # Nếu không chuyển được, bỏ qua
                continue

            # Nếu THÁNG và NĂM KHỚP với ngày hiện tại (ví dụ: 12/2025)
            if month_int == self.current_month_int and year_str == self.current_year_str:
                self.logger.info(f"✅ Đã tìm thấy nhóm nội dung cho tháng hiện tại: {month_str}/{year_str}")
                
                # 1. VÒNG LẶP QUA TẤT CẢ CÁC THÔNG BÁO TRONG KHỐI ĐÃ KHỚP
                documents = group.css('.b_right .b-right-download')
                
                for doc in documents:
                    # Lấy Tiêu đề và Ngày (cần xử lý chuỗi sau)
                    title_with_date_raw = doc.css('a span::text').get()
                    if not title_with_date_raw:
                        continue
                        
                    title_with_date_raw = title_with_date_raw.strip()
                    
                    # Lấy URL Tải về (tương đối)
                    download_url_relative = doc.css('a::attr(href)').get()
                    
                    # Xử lý URL tuyệt đối
                    download_url_absolute = response.urljoin(download_url_relative)
                    
                    # Phân tách Ngày và Tiêu đề
                    date_part = ''
                    title_part = title_with_date_raw
                    # Giả định Ngày nằm ở đầu chuỗi và theo format DD/MM/YYYY
                    if re.match(r'\d{1,2}/\d{1,2}/\d{4}', title_with_date_raw):
                        try:
                            # Tách Ngày ra khỏi Tiêu đề
                            date_part, title_part = title_with_date_raw.split(' ', 1)
                        except ValueError:
                            # Không tìm thấy khoảng trắng để tách (ví dụ: "12/2025Tiêu đề")
                            pass
                        
                    e_item = EventItem()
                    e_item['mcp'] = 'TPB'
                    e_item['web_source'] = self.allowed_domains[0]
                    # Chuyển đổi ngày tháng DD/MM/YYYY sang ISO 8601 YYYY-MM-DD
                    e_item['date'] = convert_date_to_iso8601(date_part) 
                    e_item['summary'] = title_part
                    e_item['details_raw'] = title_part + '\n' + download_url_absolute
                                        
                    yield e_item
                    
                # Dừng lại sau khi xử lý xong khối tháng hiện tại
                return 

# Hàm chuyển đổi ngày tháng (giữ nguyên)
from datetime import datetime

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None

    input_format = '%d/%m/%Y'
    output_format = '%Y-%m-%d'

    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        iso_date_str = date_object.strftime(output_format)
        return iso_date_str
    
    except ValueError as e:
        print(f"⚠️ Lỗi chuyển đổi ngày tháng '{vietnam_date_str}' (phải là DD/MM/YYYY): {e}")
        return None