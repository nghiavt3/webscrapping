import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_shs'
    mcpcty = 'SHS'
    # Thay thế bằng domain thực tế
    allowed_domains = ['archive.shs.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://archive.shs.com.vn/ShareHolder.aspx'] 

    def parse(self, response):
        #1. Trích xuất các tin nổi bật ở phần đầu (nếu có)
        for head_news in response.css('div.textnews'):
            title= head_news.css('a::text').get(default='').strip(),
            url= response.urljoin(head_news.css('a::attr(href)').get()),
            publish_date= head_news.css('span.timestamp::text').get(default='').strip()

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title[0]
            e_item['details_raw'] = str(title[0]) +'\n'  + str(url)
            e_item['date'] = convert_date_to_iso8601(publish_date)               
            yield e_item
        timestamps = response.css('span.timestamp')

        for ts in timestamps:
            # Lấy text của ngày tháng
            raw_date = ts.css('::text').get()
            
            # Bước 2: Tìm thẻ <a> chứa tiêu đề nằm cùng cấp hoặc trong cùng ô với timestamp
            # Chúng ta tìm thẻ <a> có link chứa chữ 'News'
            # XPath này tìm trong phạm vi cha của timestamp để chắc chắn lấy đúng link đi kèm
            row = ts.xpath('..') # Nhảy lên thẻ cha (thường là td hoặc span)
            title_node = row.xpath('.//a[contains(@href, "News")]')
            
            title_text = title_node.xpath('string(.)').get() # Lấy toàn bộ text bên trong thẻ a
            link = title_node.xpath('./@href').get()
            if title_text and "Tin tức" not in title_text:
                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = title_text.strip()
                e_item['details_raw'] = str(title_text.strip()) +'\n'  + str(link)
                e_item['date'] = convert_date_to_iso8601(raw_date)  
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
    vietnam_date_str = vietnam_date_str.replace('SA', 'AM').replace('CH', 'PM')
    vietnam_date_str=vietnam_date_str.strip()
    # Định dạng đầu vào: Ngày/Tháng/Năm ('%d/%m/%Y')
    input_format = '%d/%m/%Y %I:%M %p'
    
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
