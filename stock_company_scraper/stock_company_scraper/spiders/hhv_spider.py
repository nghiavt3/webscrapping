import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_hhv'
    mcpcty = 'HHV'
    allowed_domains = ['hhv.com.vn'] 
    start_urls = ['https://hhv.com.vn/cong-bo-thong-tin/'] 
    async def start(self):
        urls = [
            ('https://hhv.com.vn/cong-bo-thong-tin/', self.parse),  
            ('https://hhv.com.vn/bao-cao-tai-chinh/', self.parse_bctc),     
        ]
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                callback=callback,
                #meta={'playwright': True}
            )
    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def parse(self, response):
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

        # 2. Lấy tất cả các hàng dữ liệu trong bảng Ninja Tables
        table_rows = response.css('table.ninja_footable tbody tr')

        for row in table_rows:
            # Trích xuất Tiêu đề và URL (cùng nằm trong cột 1)
            title = row.css('td:nth-child(1) a::text').get(default='').strip()
            url_raw = row.css('td:nth-child(1) a::attr(href)').get(default='').strip()
            
            # Trích xuất Ngày (cột 2)
            date_raw = row.css('td:nth-child(2)::text').get(default='').strip()

            if not title:
                continue

            iso_date = convert_date_to_iso8601(date_raw)
            full_url = response.urljoin(url_raw)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{title}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

    async def parse_bctc(self, response):
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

        # 2. Lấy tất cả các hàng dữ liệu trong bảng Ninja Tables
        rows = response.css('tr.plus-table-row')

        for row in rows:
            file_url = row.css('td[data-title="Văn bản"] a::attr(href)').get()
            if file_url:
                # 2. Trích xuất tên báo cáo
                title = row.css('td[data-title="Văn bản"] .plus-table__text-inner::text').get()
                
                # 3. Trích xuất ngày phát hành
                # Sử dụng data-title để nhắm chính xác cột, tránh nhầm lẫn
                date_issue = row.css('td[data-title="Ngày phát hành"] .plus-table__text-inner::text').get()

                if not title:
                    continue

                iso_date = convert_date_to_iso8601(date_issue)
                full_url = response.urljoin(file_url)

                # -------------------------------------------------------
                # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
                # -------------------------------------------------------
                event_id = f"{title}_{iso_date}".replace(' ', '_').strip()[:150]
                
                cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
                if cursor.fetchone():
                    self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT.")
                    break 

                # 4. Yield Item
                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = title
                e_item['date'] = iso_date
                e_item['details_raw'] = f"{title}\nLink: {full_url}"
                e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    input_format = '%d/%m/%Y'
    output_format = '%Y-%m-%d'
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        return date_object.strftime(output_format)
    except ValueError:
        return None