import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
import json
class EventSpider(scrapy.Spider):
    name = 'event_nth'
    mcpcty = 'nth' 
    allowed_domains = ['thuydiennuoctrong.com.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def start(self):
        urls = [
            ('http://www.thuydiennuoctrong.com.vn/Home/GetDanhSachTinTucTrangChuTheoChuyenMuc?page=1&id=3135485', self.parse_generic),
            
        ]
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                callback=callback,
                headers={
                    'Accept': 'application/json',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
            )

    async def parse_generic(self, response):
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
        
        # 2. Chọn các khối tin tức dựa trên style border-bottom
        try:
            raw_data = json.loads(response.text)
        except Exception as e:
            self.logger.error(f"Lỗi parse JSON: {e}")
            return

        # Kiểm tra trạng thái Status và dữ liệu trong key 'Data'
        if "rows" in raw_data:
            items_list = raw_data["rows"]

            for item in items_list:
                # Trích xuất các trường dữ liệu
                title = item.get("TieuDe")
                create_date = item.get("NgayXuatBan")[:10] # Định dạng DD/MM/YYYY
                file_path = f'http://www.thuydiennuoctrong.com.vn/xemchitiet/{item.get("FriendUrl")}' 

                summary = title.strip()
                iso_date = convert_date_to_iso8601(create_date)
                absolute_url = (file_path)

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