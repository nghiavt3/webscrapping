import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_vre'
    mcpcty = 'VRE'
    # Website IR của Vincom Retail
    allowed_domains = ['ir.vincom.com.vn'] 

    async def start(self):
        urls = [
            ('https://ir.vincom.com.vn/cong-bo-thong-tin/cong-bo-thong-tin-vi/', self.parse),
            ('https://ir.vincom.com.vn/bao-cao-tai-chinh-va-tom-tat-ket-qua-kinh-doanh/', self.parse_bctc),
        ]
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                callback=callback,
                meta={'playwright': True}
            )

    
    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # 1. Thiết lập kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Duyệt qua danh sách tin tức (layout cột)
        for post in response.css('div.post-list-resource div.column'):
            item_node = post.css('div.item')
            
            # Trích xuất Tiêu đề: Ưu tiên lấy từ attribute title của thẻ a
            title = item_node.css('h6 a::attr(title)').get()
            if not title:
                title = item_node.css('h6 a::text').get()

            # Trích xuất URL tài liệu
            url = item_node.css('h6 a::attr(href)').get()

            # Trích xuất Ngày: Lấy định dạng máy (ISO) từ attribute datetime
            raw_datetime = item_node.css('time::attr(datetime)').get()
            iso_date = raw_datetime.split('T')[0] if raw_datetime else None

            if not title or not iso_date:
                continue

            summary = title.strip()

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['date'] = iso_date
            
            full_url = response.urljoin(url) if url else "N/A"
            e_item['details_raw'] = f"{summary}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()
        
    def parse_bctc(self, response):
        # 1. Thiết lập kết nối SQLite
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
        active_year = response.css('div.owl-item figure a.active::attr(title)').get()
        # Chọn bảng mục tiêu qua class 'border'
        table = response.css('table.border')
        
        # Lấy danh sách các quý từ tiêu đề (Q1, Q2, Q3, Q4)
        quarters = table.css('tr:first-child th:not(:first-child)::text').getall()
        
        # Duyệt qua từng hàng dữ liệu (bỏ qua hàng tiêu đề và hàng phân loại màu đỏ)
        rows = table.css('tr')
        
        for row in rows:
            # Kiểm tra nếu hàng có chứa dữ liệu (không phải hàng tiêu đề đỏ)
            row_title = row.css('td:first-child::text').get()
            
            # Bỏ qua các hàng trống hoặc hàng tiêu đề danh mục (có màu nền d33039)
            if not row_title or row.css('td[style*="background-color: #d33039"]'):
                continue
            
            # Duyệt qua các cột từ 2 đến 5 (tương ứng Q1 -> Q4)
            for index, q_name in enumerate(quarters):
                cell = row.css(f'td:nth-child({index + 2})')
                
                # Trích xuất Link (có thể là PDF hoặc link bài viết "Xem thêm")
                link = cell.css('a::attr(href)').get()
                
                # Trích xuất Ngày (nằm trong div bên dưới link)
                date = cell.css('div::text').get()
                
                if link:
                    summary = f"{active_year}-{row_title.strip()}-{q_name.strip()}"
                    iso_date = date.strip() if date else None
                    full_url = response.urljoin(link) if link else ""
                    # -------------------------------------------------------
                    # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
                    # -------------------------------------------------------
                    event_id = f"{summary}_{iso_date if iso_date else 'NODATE'}".replace(' ', '_').strip()[:150]
                    
                    cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
                    if cursor.fetchone():
                        self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT.")
                        break 

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