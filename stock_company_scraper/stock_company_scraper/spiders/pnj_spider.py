import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_pnj'
    mcpcty = 'PNJ'
    # Thay thế bằng domain thực tế
    allowed_domains = ['pnj.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.pnj.com.vn/quan-he-co-dong/thong-bao/'] 

    # def start_requests(self):
    #     yield scrapy.Request(
    #     url=self.start_urls[0],
    #     callback=self.parse,
    #     # Thêm meta để kích hoạt Playwright
    #     meta={'playwright': True}
    # )
        
    def parse(self, response):
        # Lấy phần tử chứa danh sách (năm 2025)
        container = response.css('div.answer')
        
        # Lấy toàn bộ nội dung HTML bên trong và tách theo thẻ <br>
        # Chúng ta dùng .get() để lấy chuỗi HTML thô
        raw_html = container.get()
        
        # Tách chuỗi HTML thành từng dòng dựa trên thẻ <br> hoặc <br />
        rows = re.split(r'<br\s*/?>', raw_html)

        for row in rows:
            # Tạo một Selector tạm thời từ dòng HTML này để trích xuất
            sel = scrapy.Selector(text=row)
            
            # 1. Trích xuất Tiêu đề (Nằm sau dấu + và trước dấu :)
            # Ví dụ: "+ TB thay đổi nhân sự (16/12/2025):"
            full_text = "".join(sel.css('::text').getall()).strip()
            
            if not full_text or '+' not in full_text:
                continue

            # 2. Trích xuất Ngày tháng bằng Regex
            date_match = re.search(r'(\d{2}/\d{2}/\d{4})', full_text)
            date = date_match.group(1) if date_match else ""

            # 3. Trích xuất danh sách File (Tải về / English Version / Tên người)
            links = []
            for a in sel.css('a'):
                links.append({
                    'label': a.css('::text').get().strip(),
                    'url': a.css('::attr(href)').get()
                })

            # Làm sạch tiêu đề (bỏ dấu + ở đầu và bỏ phần link ở cuối)
            clean_title = full_text.split('):')[0].replace('+', '').strip() + ')'

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = clean_title
            e_item['details_raw'] = str(clean_title) +'\n' + str(links)
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
