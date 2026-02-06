import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import json
class EventSpider(scrapy.Spider):
    name = 'event_tcb'
    mcpcty = 'TCB'
    allowed_domains = ['techcombank.com'] 
    start_urls = ['https://techcombank.com/nha-dau-tu/cong-bo-thong-tin/tai-lieu-doanh-nghiep'] 

    async def start(self):
        urls = [
            ('https://techcombank.com/graphql/execute.json/techcombank/viewDocumentList%3BcfPath%3D/content/dam/techcombank/master-data/vi/list-view-document/tai-lieu/', self.parse),
            ('https://techcombank.com/graphql/execute.json/techcombank/viewDocumentList%3BcfPath%3D/content/dam/techcombank/master-data/vi/list-view-document/nghi-quyet/', self.parse),
            ('https://techcombank.com/graphql/execute.json/techcombank/viewDocumentList%3BcfPath%3D/content/dam/techcombank/master-data/vi/list-view-document/thong-cao-bao-chi-dhcd/', self.parse),
            ('https://techcombank.com/graphql/execute.json/techcombank/viewDocumentList%3BcfPath%3D/content/dam/techcombank/master-data/vi/list-view-document/thong-bao-va-thu-moi/', self.parse),
            ('https://techcombank.com/graphql/execute.json/techcombank/viewDocumentList%3BcfPath%3D/content/dam/techcombank/master-data/vi/list-view-document/cong-bo-kqkd/', self.parse),
            ('https://techcombank.com/graphql/execute.json/techcombank/viewDocumentList%3BcfPath%3D/content/dam/techcombank/master-data/vi/list-view-document/hoi-dong-quan-tri/', self.parse),
            
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

    # def start_requests(self):
    #     yield scrapy.Request(
    #         url=self.start_urls[0],
    #         callback=self.parse,
    #         # Playwright cần thiết để xử lý các Custom Elements của TCB
    #         meta={'playwright': True}
    #     )
    
    # def parse(self, response):
    #     # 1. Khởi tạo SQLite
    #     conn = sqlite3.connect(self.db_path)
    #     cursor = conn.cursor()
    #     table_name = f"{self.name}"
    #     cursor.execute(f'''
    #         CREATE TABLE IF NOT EXISTS {table_name} (
    #             id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
    #             scraped_at TEXT, web_source TEXT, details_clean TEXT
    #         )
    #     ''')

    #     # 2. Duyệt qua các hàng dữ liệu (TCB sử dụng cấu trúc div.row cho danh sách)
    #     for row in response.css('div.row'):
    #         date_raw = row.css('.date span::text').get(default='').strip()
    #         title_raw = row.css('.content h4::text').get(default='').strip()
            
    #         # Trích xuất link download từ thẻ a.link
    #         relative_download = row.css('.file-download .show-document a.link::attr(href)').get()
            
    #         if not title_raw or not date_raw:
    #             continue

    #         summary = title_raw
    #         iso_date = convert_date_to_iso8601(date_raw)
    #         download_url = response.urljoin(relative_download) if relative_download else ""

    #         # -------------------------------------------------------
    #         # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
    #         # -------------------------------------------------------
    #         event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
            
    #         cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
    #         if cursor.fetchone():
    #             self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT.")
    #             break 

    #         # 4. Yield Item
    #         e_item = EventItem()
    #         e_item['mcp'] = self.mcpcty
    #         e_item['web_source'] = self.allowed_domains[0]
    #         e_item['summary'] = summary
    #         e_item['date'] = iso_date
    #         e_item['details_raw'] = f"{summary}\nDownload: {download_url}"
    #         e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
    #         yield e_item

    #     conn.close()

    def parse(self, response):
        #1. Khởi tạo SQLite
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
        # Chuyển đổi nội dung JSON từ response thành dictionary 
        raw_data = json.loads(response.text)
        
        # Truy cập vào danh sách các mục (items) 
        items = raw_data.get('data', {}).get('listViewDocumentFragmentList', {}).get('items', [])
        
        for item in items:
            # Trích xuất các trường dữ liệu cụ thể
            summary = item.get('categoryTitle', {}).get('plaintext')
            iso_date = item.get('date')
            download_url = item.get('documentPath', {}).get('_publishUrl') if item.get('documentPath') else None
            
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
    #         # -------------------------------------------------------
            # event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
            
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
            e_item['details_raw'] = download_url
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item
            
    
def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        # TCB thường dùng định dạng DD/MM/YYYY
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None