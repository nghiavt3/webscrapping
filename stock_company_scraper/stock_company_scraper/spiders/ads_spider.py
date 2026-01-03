import scrapy
import sqlite3
import os
import json
from stock_company_scraper.items import EventItem
from datetime import datetime
import re

class EventSpider(scrapy.Spider):
    name = 'event_ads'
    mcpcty = 'ADS'
    # Domain thực tế cho API
    allowed_domains = ['gateway.fpts.com.vn'] 
    # URL API lấy danh sách tin tức
    start_urls = ['https://gateway.fpts.com.vn/news/api/gateway/v1/mobile/list?folder=86&code=ADS&pageSize=8&selectedPage=1&cbtt=1&from=01-01-1970&to=01-01-3000&newsType=1'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        # Đường dẫn file db khớp với cấu trúc dự án của bạn
        self.db_path = 'stock_events.db'

    def start_requests(self):
        """Gửi request đến API với header thích hợp."""
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                headers={
                    'Accept': 'application/json',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
            )

    def parse(self, response):
        """Xử lý response JSON với logic Incremental Crawling."""
        
        # 1. Tải dữ liệu JSON
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error("Không thể decode JSON từ response!")
            return

        # 2. Kiểm tra mã lỗi và truy cập mảng dữ liệu
        if data.get("Code") != 0 or "Table1" not in data.get("Data", {}):
            self.logger.error(f"API trả về lỗi hoặc không có dữ liệu: {data.get('Message')}")
            return
            
        news_items = data["Data"]["Table1"]

        # 3. Kết nối SQLite để kiểm tra tin cũ
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY,
                mcp TEXT,
                date TEXT,
                summary TEXT,
                scraped_at TEXT,
                web_source TEXT,
                details_clean TEXT
            )
        ''')

        # 4. Trích xuất từng trường dữ liệu
        for item in news_items:
            title = item.get('Title', '').strip()
            pub_date_raw = item.get('DatePub', '')
            url = item.get('URL', '')
            
            # Làm sạch ngày tháng và tạo ID
            iso_date = convert_date_to_iso8601(pub_date_raw)

            # -------------------------------------------------------
            # 5. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{title}_{iso_date}".replace('/', '-').replace('.', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ API: [{title}]. DỪNG QUÉT GIA TĂNG.")
                break
                #return # THOÁT NGAY LẬP TỨC

            # 6. Đưa dữ liệu vào Item nếu là tin mới
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = f"{title}\nLink: {url}"
            e_item['date'] = iso_date 
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

# Hàm xử lý ngày tháng cho ADS (Định dạng API: 'DD/MM/YYYY HH:MM')
def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None

    # API của FPTS thường trả về dạng '20/09/2025 14:30'
    input_format = '%d/%m/%Y %H:%M'
    output_format = '%Y-%m-%d'

    try:
        # Tách bỏ phần millisecond nếu có (dấu chấm)
        clean_str = vietnam_date_str.split('.')[0].strip()
        date_object = datetime.strptime(clean_str, input_format)
        return date_object.strftime(output_format)
    except ValueError:
        # Fallback nếu API chỉ trả về ngày
        try:
            date_object = datetime.strptime(vietnam_date_str.split(' ')[0], '%d/%m/%Y')
            return date_object.strftime(output_format)
        except:
            return None