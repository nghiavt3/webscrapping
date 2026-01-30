import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
import json
from scrapy_playwright.page import PageMethod
class EventSpider(scrapy.Spider):
    name = 'event_pgb'
    mcpcty = 'PGB'
    allowed_domains = ['pgbank.com.vn'] 
    #https://www.pgbank.com.vn/nha-dau-tu/bao-cao-tai-chinh
    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'
    
    async def start(self):
        year = datetime.now().year
        urls = [
            ("https://www.pgbank.com.vn/api/v1/investor?type=1&page=1&year=2025&lang=vi", self.parse),
            ("https://www.pgbank.com.vn/api/v1/investor?type=3&page=1&lang=vi", self.parse),
        ]
        headers = {
            'accept': 'application/json, text/plain, */*',
            'referer': 'https://www.pgbank.com.vn/nha-dau-tu/cong-bo',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0',
            # Lưu ý: Token này có thể thay đổi theo phiên làm việc
            'x-xsrf-token': 'c6b03a991098168cd26e5c8328ca4c5cNfDAEItJCLqfN+unFHmpyXsHt61Q1tPUK4GuTomef2nYCdRd8E7iYuciV750bm3ELdHbf7i8NUBJPT/zePaIBFulWVaUqTEQOeouUyRmNru/ketApPMpeJB+3j+i4TJ4'
        }
        for url, callback in urls:
            yield scrapy.Request(
            url,
            headers=headers,
            callback=callback
        )
    async def parse(self, response):
        json_data = response.json() 
        
        # Truy cập vào danh sách bài viết theo cấu trúc: data -> data
        articles = json_data.get('data', {}).get('data', []) 
        
        for item in articles:
            # Trích xuất thông tin cơ bản
            title = item.get('title') 
            public_date = item.get('created_at') 
            post_id = item.get('id') 
            
            # Xử lý trường 'src' - đây là một chuỗi JSON lồng bên trong
            files_data = []
            src_raw = item.get('src') 
            
            if src_raw:
                try:
                    # Giải mã chuỗi JSON trong trường src để lấy danh sách file
                    src_json = json.loads(src_raw)
                    for file_item in src_json:
                        file_info = file_item.get('file', {})
                        files_data.append({
                            'file_name': file_item.get('name'),
                            'file_url': file_info.get('src'),
                            'size': file_item.get('size')
                        })
                except Exception as e:
                    self.logger.error(f"Lỗi parse src tại ID {post_id}: {e}")

        
            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['date'] = public_date[:10]
            e_item['details_raw'] = f"{title}\nLink: {files_data}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item
   