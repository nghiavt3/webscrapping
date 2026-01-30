import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
import json
from scrapy.selector import Selector
class EventSpider(scrapy.Spider):
    name = 'event_pan'
    mcpcty = 'PAN' 
    allowed_domains = ['thepangroup.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def start(self):
        urls = [
            ("https://api.thepangroup.vn/", self.parse),
            ]
        
        payload = {
            "m": "post",
            "fn": "list_posts",
            "languageCode": "vi-VN",
            "zoneId": 80,
            "pageIndex": 1,
            "pageSize": 20
        }
        headers = {
            'accept': 'application/json, text/plain, */*',
            'content-type': 'application/json;charset=UTF-8',
            'namespace': 'Web',  # GIÁ TRỊ QUAN TRỌNG NHẤT
            'origin': 'https://thepangroup.vn',
            'referer': 'https://thepangroup.vn/',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0'
        }
        """Gửi request đến API với header thích hợp."""
        for url,callback in urls:
            yield scrapy.Request(
                url=url,
                method='POST',
                body=json.dumps(payload),
                headers=headers,
                callback=callback,
            )
    def parse(self, response):
        # 1. Khởi tạo SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        #cursor.execute(f'''DROP TABLE IF EXISTS {table_name}''')
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')
        # API trả về JSON, dùng response.json() để đọc trực tiếp
        try:
            json_data = response.json()
            
            # Giả sử cấu trúc JSON trả về có danh sách bài viết trong key 'data'
            # (Bạn cần kiểm tra thực tế bằng JSON Viewer như tôi đã hướng dẫn)

            items = json_data.get('data', [])
            
            for item in items:
                title = item.get('title')
                publish_date = item.get('createdDate')
                sapo = item.get('sapo')
        
                # 2. Xử lý trường 'body' chứa HTML để lấy link PDF
                body_html = item.get('body', '')
                pdf_links = []
                
                if body_html:
                    # Dùng Selector để quét nội dung HTML trong chuỗi 'body'
                    sel = Selector(text=body_html)
                    
                    # Lấy tất cả các thẻ <a> có class 'pan-download-button' hoặc chứa link PDF
                    links = sel.css('a.pan-download-button')
                    for link in links:
                        pdf_name = link.css('::text').get()
                        pdf_url = link.css('::attr(href)').get()
                        pdf_links.append({
                            'name': pdf_name,
                            'url': pdf_url
                        })
                    
                
                    e_item = EventItem()
                    e_item['mcp'] = self.mcpcty
                    e_item['web_source'] = self.allowed_domains[0]
                    e_item['summary'] = title
                    e_item['date'] = publish_date[:10]
                    e_item['details_raw'] = f"{sapo}\nLink: {pdf_links} \n"
                    e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
                    yield e_item
        except Exception as e:
            self.logger.error(f"Lỗi khi parse JSON: {e}")

    def parse_generic(self, response):
        """Hàm parse dùng chung cho các chuyên mục của SeABank"""
        # 1. Khởi tạo SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        #cursor.execute(f'''DROP TABLE IF EXISTS {table_name}''')
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')
        # Lấy tất cả các hàng trừ hàng tiêu đề năm
        
        for item in response.css('div.item-news-thepan'):          
            title = item.css('h3.heading-block-content::text').get()
            date = item.css('span.name-date-left::text').get()
            link = item.css('div.thepancontent-content > a::attr(href)').get()
            if not title:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso8601(date)
            absolute_url = f"{response.urljoin(link)}"

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{iso_date if iso_date else 'NODATE'}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT CHUYÊN MỤC.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{summary}\nLink: {absolute_url} \n"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None