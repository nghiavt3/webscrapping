import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_jvc'
    mcpcty = 'JVC' 
    allowed_domains = ['ytevietnhat.com.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def start(self):
        urls = [
            ('https://ytevietnhat.com.vn/danh-muc-tin/thong-bao-co-dong-33', self.parse_generic),
            ('https://ytevietnhat.com.vn/danh-muc-tin/dai-hoi-dong-co-dong-35', self.parse_dhcd),
             ('https://ytevietnhat.com.vn/danh-muc-tin/bao-cao-tai-chinh-37', self.parse_dhcd),
            
        ]
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                callback=callback,
                #meta={'playwright': True}
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
        # Lấy tất cả các hàng trừ hàng tiêu đề năm
        rows = response.css('table.list-table tbody tr')
        
        for row in rows:            
            title = row.css('td:nth-child(2) a p::text').get()
            date = row.css('td:nth-child(1)::text').get()
            link = row.css('td:nth-child(2) a::attr(href)').get()
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

    async def parse_dhcd(self, response):
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
        for year_node in response.css('a.showdetail'):
            year_title = year_node.css('p::text').get().strip()
            # Lấy ID (ví dụ: #show_1976 -> show_1976) để tìm vùng chứa tài liệu tương ứng
            target_id = year_node.attrib.get('href').replace('#', '')
            
            # 2. Tìm vùng chứa tài liệu có ID tương ứng
            content_div = response.css(f'div#{target_id}')
            
            # 3. Trích xuất danh sách các tài liệu bên trong
            documents = []
            # Duyệt qua các thẻ p chứa link tải
            for p in content_div.css('.divToggle p, .divToggle div[id^="input_line"]'):
                doc_name = "".join(p.css('::text').getall()).strip()
                doc_link = p.css('a::attr(href)').get()
                date = p.css('::text').re_first(r'\[(\d{2}/\d{2}/\d{4})\]')
                # Chỉ thêm nếu có tên và link (loại bỏ các dòng kẻ ngang hoặc dòng trống)
                if doc_link and "Download" in doc_name:
                    


                

                    summary = doc_name
                    iso_date = convert_date_to_iso8601(date)
                    absolute_url = f"{response.urljoin(doc_link)}"

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