import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime,timedelta
import re
class EventSpider(scrapy.Spider):
    name = 'event_vpx'
    mcpcty = 'VPX'
    # Thay thế bằng domain thực tế
    allowed_domains = ['vpbanks.com.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.vpbanks.com.vn/quan-he-co-dong'] 

    def parse(self, response):
        
        # Mỗi tin bài nằm trong một wrapper item-link-wrapper
        for post in response.css('div.item-link-wrapper'):
            
            # 1. Tiêu đề: Sử dụng data-hook="post-title"
            title = post.css('[data-hook="post-title"] h2::text').get()
            
            # 2. Đường dẫn: Thẻ <a> bao quanh tiêu đề
            url = post.css('a[data-hook="post-list-item__title"]::attr(href)').get()
            
            # 3. Mô tả ngắn (Summary): Sử dụng data-hook="post-description"
            summary = post.css('[data-hook="post-description"] div.BOlnTh::text').get()
            
            # 4. Danh mục (Category):
            category = post.css('[data-hook="post-category-label"] a::text').get()
            
            # 5. Ngày đăng (Dạng "2 ngày trước" hoặc ngày cụ thể)
            # Wix thường để title của span là ngày đầy đủ
            publish_date_raw = post.css('[data-hook="time-ago"]::text').get()
            full_date = post.css('[data-hook="time-ago"]::attr(title)').get()
            
            if title:
                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = title.strip()
                e_item['details_raw'] = str(title.strip()) +'\n' +str(summary.strip() if summary else None) +'\n' + str(response.urljoin(url) if url else None)
                e_item['date'] = convert_to_iso_date(publish_date_raw)               
                yield e_item

from datetime import datetime

def convert_to_iso_date(date_str):
    if not date_str:
        return None
    
    now = datetime.now()
    date_str = date_str.lower().strip()

    # 1. Xử lý dạng "X ngày trước"
    if 'ngày trước' in date_str:
        days = int(re.search(r'\d+', date_str).group())
        target_date = now - timedelta(days=days)
        return target_date.strftime('%Y-%m-%d')

    # 2. Xử lý dạng "X giờ trước" hoặc "X phút trước"
    if 'giờ trước' in date_str or 'phút trước' in date_str:
        return now.strftime('%Y-%m-%d') # Coi như là ngày hôm nay

    # 3. Xử lý dạng "12 thg 12" hoặc "12/12"
    # Định dạng này thường thiếu năm, ta sẽ mặc định lấy năm hiện tại
    match_date = re.search(r'(\d+)\s*(?:thg|/)\s*(\d+)', date_str)
    if match_date:
        day = int(match_date.group(1))
        month = int(match_date.group(2))
        # Giả định năm hiện tại (2025)
        year = now.year
        try:
            target_date = datetime(year, month, day)
            return target_date.strftime('%Y-%m-%d')
        except ValueError:
            return None

    return date_str # Trả về nguyên bản nếu không khớp định dạng nào
