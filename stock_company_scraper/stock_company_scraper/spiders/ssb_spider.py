import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_ssb'
    mcpcty= 'SSB'

    # Thay thế bằng domain thực tế
    allowed_domains = ['seabank.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    #start_urls = ['https://www.vib.com.vn/vn/nha-dau-tu/cong-bo-thong-tin','https://www.vib.com.vn/vn/nha-dau-tu/thong-tin-co-dong'] 

    # def start_requests(self):
    #     yield scrapy.Request(
    #     url=self.start_urls[0],
    #     callback=self.parse,
    #     # Thêm meta để kích hoạt Playwright
    #     meta={'playwright': True}
    # )
    
    def start_requests(self):
        urls = [
            ('https://www.seabank.com.vn/nha-dau-tu/cong-bo-thong-tin', self.parse_cong_bo),
            ('https://www.seabank.com.vn/nha-dau-tu/thong-tin-co-dong', self.parse_co_dong),
        ]
        for url, callback in urls:
            yield scrapy.Request(url=url, callback=callback,meta={'playwright': True})

    def parse_co_dong(self, response):
        # Chúng ta chọn các section hiển thị trên Desktop để lấy đủ Title, Link, Summary và Date
        articles = response.css('section.hidden.md\:block')

        for article in articles:
            # Lấy link bài viết
            relative_url = article.css('a::attr(href)').get()
            absolute_url = response.urljoin(relative_url)

            # Lấy tiêu đề
            title = article.css('h2::text').get()

            # Lấy mô tả ngắn (đoạn văn bản tóm tắt)
            summary = article.css('p.line-clamp-3::text').get()

            # Lấy ngày đăng (nằm trong thẻ p cuối cùng có text chứa ngày tháng)
            # Selector này tìm thẻ p có class text-gray2 và chứa text trực tiếp
            date = article.css('p.text-gray2::text').re_first(r'\d{2}/\d{2}/\d{4}')
            # Lọc lấy phần tử có nội dung (vì có thể dính các thẻ p trống hoặc icon)
            clean_date = "".join(date).strip()
            e_item = EventItem()
            # 2. Trích xuất dữ liệu chi tiết
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['date'] = convert_date_to_iso8601(clean_date)
            e_item['summary'] = title.strip() if title else None
            e_item['details_raw'] = str(title.strip() if title else None) + '\n' + str(summary.strip() if summary else None) + '\n' + str(absolute_url)         
            yield e_item    

    def parse_cong_bo(self, response):
        items = response.css('section.hidden.md\:block')

        for item in items:
            # 1. Tiêu đề bài viết
            title = item.css('h2::text').get()
            
            # 2. Đường dẫn (URL) - Chuyển từ tương đối sang tuyệt đối
            link = item.css('a::attr(href)').get()
            absolute_url = response.urljoin(link) if link else None
            
            # 3. Mô tả ngắn (Summary)
            summary = item.css('p.line-clamp-3::text').get()
            
            # 4. Ngày đăng (Nằm trong thẻ p cùng hàng với icon calendar)
            # Chúng ta lấy text của thẻ p có class text-gray2 nằm trong div dưới cùng
            date = item.css('div.flex.justify-between p.text-gray2::text').get()
            
            # 5. Link ảnh đại diện
            image_url = item.css('img.object-cover::attr(src)').get()

            e_item = EventItem()
            # 2. Trích xuất dữ liệu chi tiết
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['date'] = convert_date_to_iso8601(date)
            e_item['summary'] = title
            e_item['details_raw'] = str(title) + '\n' + str(summary) + '\n' + str(absolute_url)          
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
