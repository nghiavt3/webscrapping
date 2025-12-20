import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_aas'
    mcpcty = 'AAS'
    # Thay thế bằng domain thực tế
    allowed_domains = ['aas.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://aas.com.vn/danh-muc-thong-tin-co-dong/cong-bo-thong-tin/'] 

    def start_requests(self):
        yield scrapy.Request(
        url=self.start_urls[0],
        callback=self.parse,
        # Thêm meta để kích hoạt Playwright
        meta={'playwright': True}
    )
    
    def parse(self, response):
        # Lặp qua từng item tin tức
        items = response.css('.research-item.news')
        
        for item in items:
            # Trích xuất dữ liệu cơ bản
            title = item.css('.content a.text-body-lg-semibold::text').get()
            detail_url = item.css('.content a.text-body-lg-semibold::attr(href)').get()
            
            # Lấy ngày đăng (thường là thẻ p cuối cùng trong cụm icon calendar)
            publish_date = item.css('.content .flex.items-center.gap-2 p::text').get()
            
            # Lấy nội dung tóm tắt
            summary = item.css('.content p.text-body-sm-regular.text-text-tertiary::text').get()
            
            # Trích xuất danh sách tài liệu đính kèm (nếu có)
            attachments = []
            for doc in item.css('a.link-green'):
                doc_name = doc.css('::text').get()
                doc_link = doc.css('::attr(href)').get()
                if doc_link:
                    attachments.append({
                        'file_name': doc_name.strip() if doc_name else "N/A",
                        'file_url': response.urljoin(doc_link)
                    })
            

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = str(title) +'\n' + str(summary)+'\n' + str(detail_url)
            e_item['date'] = convert_date_to_iso8601(publish_date.strip())               
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
