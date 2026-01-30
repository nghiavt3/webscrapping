import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
import json
class EventSpider(scrapy.Spider):
    name = 'event_abc'
    mcpcty = 'ABC' 
    allowed_domains = ['vmgmedia.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        
             
        yield scrapy.Request(
                url='https://vmgmedia.vn/cong-bo-thong-tin/', 
                callback=self.parse_generic,
                meta={'playwright': True}
            )
        yield scrapy.Request(
                url='https://vmgmedia.vn/wp-admin/admin-ajax.php?juwpfisadmin=false&action=wpfd&task=categories.getCats&dir=34', 
                callback=self.parse_categories,
                
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
        articles = response.css('article.elementor-post')
        
        for article in articles:       
            title = article.css('.elementor-post__title a::text').get()
            date = article.css('.elementor-post-date::text').get()
            link = article.css('.elementor-post__title a::attr(href)').get()
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
    
    def parse_categories(self, response):
        try:
            # Bước 2: Chuyển đổi dữ liệu JSON trả về
            result = json.loads(response.text)
            data = result.get('data')
            children = data[0].get('children')
            bctc = children[0]
            namtcs= bctc.get('children')

            for ntc in namtcs:
                quytcs= ntc.get('children')
                for quy in quytcs:
                    term_id = quy.get('term_id')

                    # Bước 3: Build URL để lấy danh sách file của từng Category
                    # Dựa vào tham số thường thấy của plugin này: task=files.display & category_id
                    file_api_url = f"https://vmgmedia.vn/wp-admin/admin-ajax.php?juwpfisadmin=false&action=wpfd&task=files.display&view=files&id={term_id}&rootcat=34&page=&orderCol=created_time&orderDir=desc&page_limit=10"
                    
                    yield scrapy.Request(
                        url=file_api_url,
                        callback=self.parse_files,
                #         headers={
                #     'Accept': 'application/json',
                #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                # }
                    )
        except Exception as e:
            self.logger.error(f"Lỗi phân tích JSON: {e}")
    
    def parse_files(self, response):
        # 1. Khởi tạo kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Giải mã JSON
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error("Lỗi giải mã JSON!")
            return

            
        news_items = data["files"]

        # 3. Trích xuất và kiểm tra trùng lặp
        for item in news_items:
            title = item.get('post_title')
            pub_date = item.get('created_time')
            url = item.get('linkdownload')

            if not title or not pub_date:
                continue

            iso_date = (pub_date)
            summary = title.strip()

            # --- KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC) ---
            event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT.")
                break 

            # 4. Gán Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{summary}\nLink: {url}"
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