import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_dxg'
    mcpcty = 'DXG'
    # Thay thế bằng domain thực tế
    allowed_domains = ['ir.datxanh.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://ir.datxanh.vn/cong-bo-thong-tin'] 

    def parse(self, response):
        container = response.css('#pills-unusual .filter-section-content')

        # 2. Lặp qua từng khối Năm (div.row.mb-4.year-element)
        for year_block in container.css('.year-element'):
            year = year_block.attrib.get('value', 'N/A')

            # 3. Lặp qua từng mục thông tin (div.vanban-cbttbt.search-element)
            for item in year_block.css('.vanban-cbttbt.search-element'):
                # Trích xuất dữ liệu
                title = item.css('a.search-query::text').get()
                detail_url = item.css('a.search-query::attr(href)').get()
                
                # Khối chứa Ngày và Link Tải xuống
                date_download_block = item.css('.accordion-body')
                
                # Ngày công bố (span trong khối)
                date_published = date_download_block.css('span::text').get()
                
                # URL tải xuống (thẻ a cuối cùng trong khối)
                download_url = date_download_block.css('a[download]::attr(href)').get()
                
                # Trích xuất thêm thông tin lọc (nếu cần)
                quarter = item.attrib.get('value1', 'N/A')
                type_doc = item.attrib.get('value2', 'N/A')

                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = title.strip()
                e_item['details_raw'] = title.strip() +'\n' + str(response.urljoin(detail_url) if detail_url else None) + '\n' + str(response.urljoin(download_url) if download_url else None)
                e_item['date'] = convert_date_to_iso8601(date_published.strip())
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
    input_format = "%d/%m/%Y"    
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
