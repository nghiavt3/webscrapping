import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_gas'
    mcpcty = 'GAS'
    allowed_domains = ['pvgas.com.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        urls = [
            ('https://www.pvgas.com.vn/quan-he-co-%C4%91ong', self.parse_generic),
            #('https://bsr.com.vn/cong-bo-thong-tin-khac', self.parse_generic),
            
        ]
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                callback=callback,
                #meta={'playwright': True}
            )

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

        # Lấy tất cả các khối bài viết
        articles = response.css('div.EDN_article')

        for art in articles:
            title = art.css('h3.simpleArticleTitle a::text').get()
            raw_date = art.css('span.EDN_simpleDate::text').get()
            clean_date = raw_date.replace('Đăng ngày:', '').strip() if raw_date else None

            documents = []
            doc_links = art.css('div.edn_articleDocuments ul li')
            
            for doc in doc_links:
                doc_name = doc.css('a::text').get()
                doc_url = doc.css('a::attr(href)').get()
                
                if doc_url:
                    documents.append({
                        'file_name': doc_name.strip() if doc_name else "No Name",
                        'file_url': response.urljoin(doc_url) # Nối domain vào link tải
                    })

            if not title or not clean_date:
                continue

            summary = title.strip()
            iso_date = parse_vn_date_simple(clean_date)
            absolute_url = documents[0].get('file_url')

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
            
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
            e_item['details_raw'] = f"{summary}\nLink: {absolute_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()
import unicodedata
def parse_vn_date_simple(date_str):
    if not date_str:
        return None
    
    # 1. Làm sạch: bỏ "Đăng ngày:", thay thế các ký tự xuống dòng bằng khoảng trắng
    clean_str = date_str.replace("Đăng ngày:", "").replace("\n", " ").replace("\r", " ").strip()
    
    # 2. Tách chuỗi thành mảng các từ
    parts = clean_str.split() # Ví dụ: ['30', 'Tháng', 'Chín', '2025']
    
    # Nếu không đủ 4 thành phần thì không phải format chuẩn
    if len(parts) < 4:
        return date_str

    day = parts[0].zfill(2)   # "30"
    month_raw = parts[2]      # "Chín" hoặc "Mười"
    year = parts[3]           # "2025"

    # Trường hợp đặc biệt: "Mười Một" hoặc "Mười Hai" sẽ làm mảng dài hơn (5 phần tử)
    if len(parts) == 5:
        month_raw = f"{parts[2]} {parts[3]}" # Ghép "Mười" + "Hai"
        year = parts[4]

    # 3. Bảng tra cứu tháng
    month_map = {
        "Một": "01", "Hai": "02", "Ba": "03", "Tư": "04",
        "Năm": "05", "Sáu": "06", "Bảy": "07", "Tám": "08",
        "Chín": "09", "Mười": "10", "Mười Một": "11", "Mười Hai": "12",
        "1": "01", "2": "02", "3": "03", "4": "04", "5": "05", "6": "06",
        "7": "07", "8": "08", "9": "09", "10": "10", "11": "11", "12": "12"
    }

    # 1. Chuẩn hóa month_raw về NFC (Dựng sẵn) và xóa khoảng trắng thừa
    month_raw_clean = unicodedata.normalize('NFC', month_raw).strip()
    
    # 2. Chuẩn hóa tất cả Key trong month_map về NFC để đảm bảo khớp 100%
    # (Đôi khi code bạn viết ở editor dùng chuẩn khác với dữ liệu web)
    month_map_nfc = {unicodedata.normalize('NFC', k): v for k, v in month_map.items()}

    # 3. Thực hiện lấy dữ liệu
    month = month_map_nfc.get(month_raw_clean)

    #month = month_map.get(month_raw)
    
    return f"{year}-{month}-{day}"