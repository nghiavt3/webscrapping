import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import json
class EventSpider(scrapy.Spider):
    name = 'event_ntc'
    mcpcty = 'NTC'
    allowed_domains = ['namtanuyen.com.vn'] 
    current_year= datetime.now().year
    last_year= current_year-1
    start_urls = [
                  f'https://api.namtanuyen.com.vn/api/EnvInfo/Search?menuId=ffcafbf6-8f24-47d4-9f25-177d68aa0ba4&searchText=&year={current_year}',#tbcd
                  f'https://api.namtanuyen.com.vn/api/EnvInfo/Search?menuId=ffcafbf6-8f24-47d4-9f25-177d68aa0ba4&searchText=&year={last_year}',#tbcd
                  f'https://api.namtanuyen.com.vn/api/EnvInfo/Search?menuId=2d9e0bcb-b37e-4605-9796-e2497d91d354&searchText=&year={current_year}',#dhcd
                  f'https://api.namtanuyen.com.vn/api/EnvInfo/Search?menuId=2d9e0bcb-b37e-4605-9796-e2497d91d354&searchText=&year={last_year}',#dhcd
                  f'https://api.namtanuyen.com.vn/api/EnvInfo/Search?menuId=4569e9a6-eee4-4edc-8d0c-4c228da95a8b&searchText=&year={current_year}'#bctc
                  f'https://api.namtanuyen.com.vn/api/EnvInfo/Search?menuId=4569e9a6-eee4-4edc-8d0c-4c228da95a8b&searchText=&year={last_year}'#bctc
                  ] 
    async def start(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                headers={
                    'Accept': 'application/json',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
            )
    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # 1. Kết nối SQLite và chuẩn bị bảng
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
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
        if raw_data.get("Status") == 1 and "Data" in raw_data:
            items_list = raw_data["Data"]

            for item in items_list:
                # Trích xuất các trường dữ liệu
                title = item.get("Title")
                create_date = item.get("CreateDate") # Định dạng DD/MM/YYYY
                file_path = item.get("FilePath")
                
                # Chuyển đổi ngày tháng sang ISO (YYYY-MM-DD) nếu cần
                iso_date = None
                if create_date:
                    try:
                        date_obj = datetime.strptime(create_date, '%d/%m/%Y')
                        iso_date = date_obj.strftime('%Y-%m-%d')
                    except ValueError:
                        iso_date = None

                # Tạo link tải file đầy đủ
                full_file_url = response.urljoin(file_path) if file_path else None

                summary = title.strip()
                
                full_url = full_file_url

                # -------------------------------------------------------
                # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
                # -------------------------------------------------------
                # event_id = f"{summary}_{iso_date if iso_date else 'NODATE'}".replace(' ', '_').strip()[:150]
                
                # cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
                # if cursor.fetchone():
                #     self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT.")
                #     break 

                # 4. Yield Item
                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = summary
                e_item['date'] = iso_date
                e_item['details_raw'] = f"{summary}\nLink: {full_url}"
                e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        # Định dạng của NTC: 'DD/MM/YYYY HH:MM:SS'
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y %H:%M:%S')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        # Fallback nếu format ngày bị thay đổi (không có giờ)
        try:
            date_object = datetime.strptime(vietnam_date_str.strip()[:10], '%d/%m/%Y')
            return date_object.strftime('%Y-%m-%d')
        except:
            return None