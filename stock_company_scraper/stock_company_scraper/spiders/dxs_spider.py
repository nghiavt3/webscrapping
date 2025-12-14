import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_dxs'
    mcpcty = 'DXS'
    # Thay thế bằng domain thực tế
    allowed_domains = ['datxanhservices.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://datxanhservices.vn/quan-he-co-dong/cong-bo-thong-tin/'] 

    def parse(self, response):
        year_groups = response.css('div.tab-list.table-ttbt')
        
        # Lặp qua từng nhóm (Năm)
        for group in year_groups:
            # 1. Trích xuất Năm từ thẻ h3
            year_raw = group.css('h3.title::text').get()
            current_year = year_raw.strip() if year_raw else None
            
            # 2. Lặp qua từng mục tài liệu (item) trong nhóm Năm này
            document_items = group.css('div.document-items div.item')

            for item in document_items:
                # Trích xuất Tiêu đề
                title = item.css('a.item-title::text').get()
                
                # Trích xuất URL tài liệu/xem
                doc_url = item.css('a.item-title::attr(href)').get()
                
                # Trích xuất Ngày công bố
                date_raw = item.css('.item-date::text').get()
                
                # Trích xuất URL Tải về (Thẻ a có thuộc tính 'download')
                download_url = item.css('a[download]::attr(href)').get()
                
                # Làm sạch và định dạng Ngày
                #cleaned_date = self.format_date(date_raw)
                
                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = title.strip()
                e_item['details_raw'] = str(title.strip()) +'\n' + str(doc_url)+'\n' + str(download_url)
                e_item['date'] = convert_date_to_iso8601(date_raw)               
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
