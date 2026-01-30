import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_dpg'
    mcpcty = 'DPG'
    allowed_domains = ['datphuong.com.vn'] 
    

    async def start(self):
        urls = [
            ('https://www.datphuong.com.vn/documents/tap-doan-dat-phuong/cong-bo-thong-tin', self.parse),
            ('https://www.datphuong.com.vn/documents/tap-doan-dat-phuong/dai-hoi-co-dong', self.parse),
            ('https://www.datphuong.com.vn/documents/tap-doan-dat-phuong/bao-cao-tai-chinh', self.parse_generic),
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

        # 2. Lặp qua từng mục tin tức/PDF (.pdf-item)
        items = response.css('.pdf-item')
        
        for item in items:
            title_text = item.css('h4 a::text').get()
            pdf_url = item.css('h4 a::attr(href)').get()
            # Lấy ngày nằm trong span có class font-500
            date_raw = item.css('.pdf-action span.font-500::text').get()
            
            if not title_text:
                continue

            cleaned_title = title_text.strip()
            iso_date = convert_date_to_iso8601(date_raw)
            # Tạo link tuyệt đối cho file PDF
            full_pdf_url = response.urljoin(pdf_url)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{cleaned_title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item để truyền qua Pipeline
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{cleaned_title}\nLink PDF: {full_pdf_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

    def parse_generic(self, response):
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

        # 2. Lặp qua từng mục tin tức/PDF (.pdf-item)
        groups = response.css('div.pdf-group')
        for group in groups:
            group_name = group.css('div.pdf-t::text').get()
            # Lặp qua từng file cụ thể trong nhóm đó
            items = group.css('div.pdf-item')
            for item in items:
                # 1. Trích xuất tiêu đề báo cáo
                title = item.css('h4 a::text').get()
                
                # 2. Trích xuất link PDF
                pdf_link = item.css('h4 a::attr(href)').get()
                
                # 3. Trích xuất ngày công bố
                date = item.css('span.font-500::text').get()
        
            
                if not title:
                    continue

                cleaned_title = title.strip()
                summary = cleaned_title
                iso_date = None
                # Tạo link tuyệt đối cho file PDF
                full_pdf_url = response.urljoin(pdf_link)

                # -------------------------------------------------------
                # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
                # -------------------------------------------------------
                event_id = f"{summary}_{iso_date if iso_date else 'NODATE'}".replace(' ', '_').strip()[:150]
                
                cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
                if cursor.fetchone():
                    self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                    break 

                # 4. Yield Item để truyền qua Pipeline
                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = summary
                e_item['date'] = iso_date
                e_item['details_raw'] = f"{summary}\nLink PDF: {full_pdf_url}"
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