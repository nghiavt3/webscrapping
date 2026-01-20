import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_tnt'
    mcpcty = 'TNT' 
    allowed_domains = ['tnt-group.vn']

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        urls = [
            ('https://tnt-group.vn/cong-bo-thong-tin/', self.parse_generic),
            ('https://tnt-group.vn/category/quan-he-co-dong/', self.parse_qhcd),
            ('https://tnt-group.vn/category/dai-hoi-co-dong/', self.parse_qhcd),
             
            
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
        # Lấy tất cả các hàng trừ hàng tiêu đề năm
        items = response.css('div.tab-list div.item')
        
        for item in items:           
            title = item.css('div.name a span:not(.date_mobile)::text').get()
            date = item.css('div.date::text').get()
            link = item.css('div.link a::attr(href)').get()
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

    def parse_qhcd(self, response):
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
        articles = response.css('article.zek_item_news')
        
        for article in articles:          
            title = article.css('h3.name a::text').get().strip()
            date = article.css('.time::text').get().strip()
            link = article.css('h3.name a::attr(href)').get()
            if not title:
                continue

            summary = title.strip()
            iso_date = parse_vietnamese_date(date)
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
    
def parse_vietnamese_date(date_str):
    if not date_str:
        return None
        
    # Từ điển ánh xạ tháng tiếng Việt sang số
    month_mapping = {
        "tháng một": "1", "tháng giêng": "1", "tháng 1": "1",
        "tháng hai": "2", "tháng 2": "2",
        "tháng ba": "3", "tháng 3": "3",
        "tháng tư": "4", "tháng 4": "4",
        "tháng năm": "5", "tháng 5": "5",
        "tháng sáu": "6", "tháng 6": "6",
        "tháng bảy": "7", "tháng 7": "7",
        "tháng tám": "8", "tháng 8": "8",
        "tháng chín": "9", "tháng 9": "9",
        "tháng mười": "10", "tháng 10": "10",
        "tháng mười một": "11", "tháng 11": "11",
        "tháng mười hai": "12", "tháng 12": "12"
    }

    # Chuyển về chữ thường để so sánh
    date_str_lower = date_str.lower().replace(",", "") # Xóa dấu phẩy nếu có
    
    # Thay thế chữ "Tháng..." bằng số tương ứng
    for vn_month, month_num in month_mapping.items():
        if vn_month in date_str_lower:
            date_str_lower = date_str_lower.replace(vn_month, month_num)
            break
    
    try:
        # Sau khi thay thế, chuỗi sẽ có dạng "4 27 2020"
        # Ta dùng định dạng "%m %d %Y" để parse
        dt = datetime.strptime(date_str_lower.strip(), "%m %d %Y")
        return dt.date().isoformat() # Trả về "2020-04-27"
    except Exception:
        return None # Trả về chuỗi gốc nếu lỗi