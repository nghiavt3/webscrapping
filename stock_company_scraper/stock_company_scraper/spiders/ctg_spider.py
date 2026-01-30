import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_ctg'
    mcpcty = 'CTG'
    allowed_domains = ['investor.vietinbank.vn'] 
   # start_urls = ['https://investor.vietinbank.vn/Filings.aspx'] 

    async def start(self):
        urls = [
            #('https://investor.vietinbank.vn/Filings.aspx', self.parse),
             ('https://investor.vietinbank.vn/vi/download.aspx', self.parse_bctc),
             ('https://investor.vietinbank.vn/vi/extraordinaryreports.aspx', self.parse_cbtt),
             ('https://investor.vietinbank.vn/vi/periodicreports.aspx', self.parse_cbtt),
             ('https://investor.vietinbank.vn/vi/shareholdermeetings.aspx', self.parse_cbtt),
             ('https://investor.vietinbank.vn/vi/otherevents.aspx', self.parse_cbtt),
              
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

    def parse(self, response):
        # 1. Kết nối SQLite và chuẩn bị bảng
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        table_name = f"{self.name}"
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

        # 2. Chọn các hàng tin tức (tr có valign="top")
        # Selector này nhắm vào bảng danh sách tài liệu công bố
        news_items = response.css('table tr[valign="top"]')

        for item in news_items:
            title_node = item.css('div.rpt_title a')
            if not title_node:
                continue

            title = title_node.css('::text').get().strip()
            url = title_node.css('::attr(href)').get()
            
            # Sử dụng Regex để trích xuất ngày DD/MM/YYYY từ span
            pub_date = item.css('div.rpt_title span::text').re_first(r'(\d{2}/\d{2}/\d{4})')

            # Làm sạch và định dạng
            iso_date = convert_date_to_iso8601(pub_date)
            full_url = response.urljoin(url)

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

    def parse_bctc(self, response):
        # 1. Kết nối SQLite và chuẩn bị bảng
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        table_name = f"{self.name}"
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

        # 2. Chọn các hàng tin tức (tr có valign="top")
        # Selector này nhắm vào bảng danh sách tài liệu công bố
        # Chọn vùng chứa lớn nhất
        container = response.css('div.dowload')
        
        # Duyệt qua từng mục báo cáo (thẻ div class "pdf")
        reports = container.css('div.pdf')

        for report in reports:

            title = report.css('h3.title-file::text').get()
            url = report.css('div.action-file a::attr(href)').get()
            
            

            # Làm sạch và định dạng
            summary = title.strip()
            iso_date = None
            full_url = response.urljoin(url)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{iso_date if iso_date else 'NODATE'}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT.")
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

    def parse_cbtt(self, response):
        # 1. Kết nối SQLite và chuẩn bị bảng
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        table_name = f"{self.name}"
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

        # 2. Chọn các hàng tin tức (tr có valign="top")
        # Selector này nhắm vào bảng danh sách tài liệu công bố
        # Chọn vùng chứa lớn nhất
        
        # Duyệt qua từng mục báo cáo (thẻ div class "pdf")
        items = response.css('div.flex.flex-col.border-b')

        for item in items:

            title = item.css('h3.title-file::text').get()
            url = item.css('a[href*="redirect"]::attr(href)').get()
            date= item.css('div.date-title::text').get()
            download_url = item.css('a[href*="download=true"]::attr(href)').get()

            # Làm sạch và định dạng
            summary = title.strip()
            iso_date = convert_date_to_iso8601(date)
            full_url = f"{response.urljoin(url)}\n{response.urljoin(download_url)}"

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{iso_date if iso_date else 'NODATE'}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT.")
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