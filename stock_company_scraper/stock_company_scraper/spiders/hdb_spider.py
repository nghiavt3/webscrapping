import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_hdb'
    mcpcty= 'HDB'

    # Thay thế bằng domain thực tế
    allowed_domains = ['hdbank.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://hdbank.com.vn/vi/investor/thong-tin-nha-dau-tu/quan-he-co-dong/cong-bo-thong-tin-thong-tin-khac'] 

    def start_requests(self):
        yield scrapy.Request(
        url=self.start_urls[0],
        callback=self.parse,
        # Thêm meta để kích hoạt Playwright
        meta={'playwright': True}
    )
    
    def parse(self, response):
        # Lấy tất cả các thẻ <p> trong vùng content, đây là các khối thông báo và ngày/giờ xen kẽ nhau
        all_paragraphs = response.css('div.content-tab__content div p')
        
        # Lặp qua các thẻ <p> theo nhóm 2 (Thông báo + Ngày/Giờ)
        for i in range(0, len(all_paragraphs), 2):
            
            # Đảm bảo có đủ 2 phần tử (thông báo và ngày)
            if i + 1 < len(all_paragraphs):
                
                announcement_p = all_paragraphs[i]    # Thẻ <p> chứa tiêu đề và link (lẻ: 0, 2, 4...)
                datetime_p = all_paragraphs[i+1]      # Thẻ <p> chứa ngày giờ (chẵn: 1, 3, 5...)
                
                
                
                # --- Trích xuất từ thẻ <p> chứa thông báo ---
                link_selector = announcement_p.css('a')
                
                # Lấy Tiêu đề (Sử dụng ::text trên thẻ <a>)
                # Dùng get() và strip() để loại bỏ khoảng trắng thừa
                title_raw = link_selector.css('::text').get()
                if title_raw:
                    title = title_raw.strip()
                
                # Lấy Link Tải về
                relative_url = link_selector.css('::attr(href)').get()
                if relative_url:
                    # Chuyển URL tương đối thành URL tuyệt đối (cần thiết cho link file)
                    url = response.urljoin(relative_url)
                
                # --- Trích xuất từ thẻ <p> chứa ngày giờ ---
                # Dùng normalize-space() để lấy toàn bộ text và loại bỏ các ký tự xuống dòng/khoảng trắng thừa
                date_time_raw = datetime_p.css('::text').getall()
                if date_time_raw:
                     # Nối các phần text lại và loại bỏ khoảng trắng không cần thiết
                    pub_date = ' '.join([t.strip() for t in date_time_raw if t.strip()])

            e_item = EventItem()
            # 2. Trích xuất dữ liệu chi tiết
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['date'] = convert_date_to_iso8601(pub_date)
            e_item['summary'] = title
            
            e_item['details_raw'] = str(title) + '\n' + str(url) 
                         
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
    input_format = 'Ngày %d/%m/%Y vào lúc %H:%M:%S'
    
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
