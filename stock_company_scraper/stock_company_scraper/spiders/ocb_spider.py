import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import json
import re
class EventSpider(scrapy.Spider):
    name = 'event_ocb'
    mcpcty = 'OCB'
    allowed_domains = ['ocb.com.vn'] 
    start_urls = ['https://ocb.com.vn/vi/nha-dau-tu/cong-bo-thong-tin'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def parse(self, response):
        with open("debug_page.html", "wb") as f:
            f.write(response.body)
        # 1. Kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
       # cursor.execute(f'''DROP TABLE IF EXISTS {table_name}''')
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 1. Tìm thẻ script chứa dữ liệu ngầm (Transfer State)
        # Thông thường ID là 'server-app-state'
        script_data = response.css('script#serverApp-state::text').get()

        if not script_data:
            self.logger.error("Không tìm thấy thẻ script dữ liệu!")
            return

        # 2. Parse chuỗi JSON
        try:
            # Lưu ý: Một số ký tự đặc biệt có thể bị escape trong HTML (ví dụ &qout;)
            # Scrapy tự động handle hầu hết các trường hợp này
            data = json.loads(script_data)
            

            # Truy cập thẳng vào key ID
            target_ids = ['815871077' ,#thongtintaichinh
                         '1242064574',#daihoicodong
                         '2372755190',#baocaothuongnien
                         '2999441261'#congbothongtin
                         ]
    
            for target_id in target_ids:
                if target_id in data:
                    body_content = data[target_id].get('body', [])
                    for item in body_content:
                        title= item.get('name')
                        link= f"https://webocb-api.ocb.com.vn/Resources/Files/{item.get('fileMedia')}"
                        date= item.get('publishDate')
                        
                        summary = title
                        # Làm sạch ngày (loại bỏ "Ngày đăng: ")
                        clean_date_str = date[:10]
                        iso_date = clean_date_str
                        full_url = link

                    # 4. Yield Item
                        e_item = EventItem()
                        e_item['mcp'] = self.mcpcty
                        e_item['web_source'] = self.allowed_domains[0]
                        e_item['summary'] = summary
                        e_item['date'] = iso_date
                        e_item['details_raw'] = f"{summary}\nLink: {full_url}"
                        e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        
                        yield e_item

        except Exception as e:
            self.logger.error(f"Lỗi khi xử lý JSON: {e}")

                

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None