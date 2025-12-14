import scrapy
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_vos'
    # Thay thế bằng domain thực tế
    allowed_domains = ['vosco.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://www.vosco.vn/vi/a/tin-tuc-co-dong-129'] 

    def parse(self, response):
        articles = response.css('article.hentry__2')   
        for article in articles:
            # Trích xuất dữ liệu cho từng bài viết
            
            # 1. Tiêu đề: Dùng .hentry__title a::text
            title = article.css('.hentry__title a::text').get()
            
            # 2. URL bài viết: Dùng .hentry__title a::attr(href)
            url = article.css('.hentry__title a::attr(href)').get()
            
            # 3. Ngày đăng: Dùng time.date::text
            date_machine = article.css('time.date::attr(datetime)').get()
            
            # 4. Tác giả: Dùng span.author::text
            author = article.css('span.author::text').get()
            
            # 5. Lượt xem: Dùng span.categories::text
            views = article.css('span.categories::text').get()

            # 6. Thông tin file đính kèm
            file_info = []
            # Chọn tất cả các thẻ <a> trong class news-file
            file_links = article.css('span.news-file a')
            for link in file_links:
                item = EventItem()
                file_info.append({
                    # Lấy thuộc tính title
                    'file_name': link.css('::attr(title)').get(),
                    # Lấy thuộc tính href
                    'file_url': link.css('::attr(href)').get()
                })   
            # --- 4. Làm sạch và Gán vào Item ---
            if title :
                item['mcp'] = 'VOS'
                item['web_source'] = 'vosco.vn'
                item['summary'] = title.strip()
                item['date'] = date_machine 
                # Bước 1: Trích xuất danh sách chỉ chứa các URL
                item['details_raw'] = str(title.strip()) +'\n'+ str(url)
                #item['details_clean'] = download_url
                #item['download_url'] = download_url
                yield item
