import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_sip'
    mcpcty = 'SIP'
    allowed_domains = ['saigonvrg.com.vn'] 
    start_urls = ['https://saigonvrg.com.vn/vi/thong-bao-co-dong',
                  'https://saigonvrg.com.vn/vi/bao-cao-tai-chinh',
                  'https://saigonvrg.com.vn/vi/dai-hoi-dong-co-dong'
                  ] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def parse(self, response):
        # 1. Khởi tạo SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Lọc các block dữ liệu chính, bỏ qua dulieu2 (thường là padding hoặc tin phụ)
        data_blocks = response.css('div.khungdl > div.dulieu:not(.dulieu2)')
        
        for block in data_blocks:
            ngay_tao = block.css('p.ngay::text').get()
            title_tag = block.css('h3 a.clickxemdulieu')
            tieu_de = title_tag.css('::text').get()
            # ID này thường dùng để gọi AJAX xem chi tiết hoặc tải file
            view_link_id = block.css('p.tttin a.xem::attr(data-id)').get()

            if not tieu_de or not ngay_tao:
                continue

            summary = tieu_de.strip()
            iso_date = convert_date_to_iso8601(ngay_tao.strip())

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Sử dụng summary và date để tạo ID duy nhất
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
            # Lưu ID tài liệu vào details để tham chiếu khi cần tải PDF
            e_item['details_raw'] = f"{summary}\nDocID: {view_link_id}"
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