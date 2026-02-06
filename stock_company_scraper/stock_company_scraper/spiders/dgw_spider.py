import scrapy
import sqlite3
import json
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_dgw'
    mcpcty = 'DGW'
    allowed_domains = ['digiworld.com.vn','gateway.fpts.com.vn'] 
    start_urls = [#'https://digiworld.com.vn/quan-he-nha-dau-tu',
                  #'https://gateway.fpts.com.vn/news/api/gateway/v1/mobile/list?folder=86&code=DGW&pageSize=8&selectedPage=1&cbtt=0&from=01-01-1970&to=01-01-3000&newsType=1',
                  #'https://gateway.fpts.com.vn/news/api/gateway/v1/mobile/list?folder=86&code=DGW&pageSize=8&selectedPage=1&cbtt=1&from=01-01-1970&to=01-01-3000&newsType=1',
                  'https://digiworld.com.vn/quan-he-nha-dau-tu?search=investorCorporateAnnouncement&year=all',
                  'https://digiworld.com.vn/quan-he-nha-dau-tu?search=investorAnnualGeneralMeeting&year=all',
                  'https://digiworld.com.vn/quan-he-nha-dau-tu?search=investorReport&year=all'
                  ] 

    async def start(self):
        """Gửi request đến API với header mô phỏng trình duyệt."""
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                headers={
                # Header quan trọng để server nhận diện đây là cuộc gọi XHR
                'X-Requested-With': 'XMLHttpRequest',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            },
                callback=self.parse
            )

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # 1. Kết nối SQLite và chuẩn bị bảng
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        table_name = f"{self.name}"
        #cursor.execute(f'''DROP TABLE IF EXISTS {table_name}''')
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

        # 2. Lặp qua các khối tin tức
        for item in response.css('div.investor-item'):
            # Trích xuất tiêu đề
            title = item.css('.investor-item__title::text').get()
            
            # Trích xuất ngày giờ (xử lý khoảng trắng và &nbsp;)
            raw_datetime = item.css('.investor-item__datetime::text').getall()
            clean_datetime = " ".join([t.strip() for t in raw_datetime if t.strip()])
            # 3. Trích xuất Link xem online (Thường là link PDF)
            view_online = item.css('a.investor-item__btn--dark::attr(href)').get().replace('\\', '/').replace(' ', '%20')
            # Trích xuất link PDF
            pdf_url = item.css('a.investor-item__btn--primary::attr(href)').get().replace('\\', '/').replace(' ', '%20')

            if not title:
                continue

            title_clean = title.strip()
            # Lấy phần ngày (bỏ phần giờ nếu có) để convert
            date_part = clean_datetime.split()[0] if clean_datetime else ""
            iso_date = convert_date_to_iso8601_2(date_part)
            full_pdf_link = f"{response.urljoin(pdf_url) if pdf_url else ""}\n{response.urljoin(view_online) if pdf_url else ""}"

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{title_clean}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title_clean}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item để Pipeline lưu DB và gửi Telegram
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title_clean
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{title_clean}\nLink tài liệu: {full_pdf_link}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

    def parse_iso(self, response):
        """Xử lý response JSON và kiểm tra tin cũ."""
        # 1. Kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Parse dữ liệu JSON
        try:
            data = json.loads(response.text)
            announcements = data.get('Data', {}).get('Table1', [])
        except (json.JSONDecodeError, KeyError):
            self.logger.error("Không thể giải mã JSON hoặc cấu trúc API thay đổi!")
            return

        # 3. Duyệt danh sách tin tức
        for item in announcements:
            title = item.get('Title', '').strip()
            date_pub = item.get('DatePub', '')
            raw_url = item.get('URL', '')
            # Làm sạch URL (thay dấu gạch chéo ngược)
            final_url = raw_url.replace('\\', '/') if raw_url else ""

            iso_date = convert_date_to_iso8601(date_pub)

            # -------------------------------------------------------
            # 4. KIỂM TRA TIN CŨ (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT.")
                break 

            # 5. Đóng gói Item truyền qua Pipeline
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{title}\nLink: {final_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    """Chuyển đổi từ 'DD/MM/YYYY HH:MM' sang 'YYYY-MM-DD'."""
    if not vietnam_date_str:
        return None

    input_format = '%d/%m/%Y %H:%M'
    output_format = '%Y-%m-%d'

    try:
        # Tách bỏ phần nano giây nếu có (ví dụ .000)
        clean_date_str = vietnam_date_str.split('.')[0].strip()
        date_object = datetime.strptime(clean_date_str, input_format)
        return date_object.strftime(output_format)
    except Exception:
        return None
    
def convert_date_to_iso8601_2(vietnam_date_str):
    if not vietnam_date_str:
        return None
    input_format = '%d/%m/%Y'
    output_format = '%Y-%m-%d'
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        return date_object.strftime(output_format)
    except ValueError:
        return None