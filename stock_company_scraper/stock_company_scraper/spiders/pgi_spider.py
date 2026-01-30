import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_pgi'
    mcpcty = 'PGI' 
    allowed_domains = ['pjico.com.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def start(self):
        urls = [
            ('https://www.pjico.com.vn/danh-muc-tai-chinh-co-dong/thong-tin-cho-co-dong', self.parse_generic),
            ('https://www.pjico.com.vn/danh-muc-tai-chinh-co-dong/dai-hoi-dong-co-dong', self.parse_generic),
             ('https://www.pjico.com.vn/danh-muc-tai-chinh-co-dong/bao-cao-tai-chinh', self.parse_generic),
             
             
            
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

        # Chọn vùng chứa chính
        main_content = response.css('div.body.insurrance')
        
        # Tìm tất cả các tiêu đề năm (accordion) và các khung nội dung tương ứng (panel)
        # Vì chúng là các thẻ anh em (siblings) nằm kế tiếp nhau
        years = main_content.css('button.accordion::text').getall()
        panels = main_content.css('div.panel')

        for year, panel in zip(years, panels):
            year_text = year.strip()
            
            # Trích xuất từng tài liệu trong năm đó
            documents = panel.css('div.list-documents ul li')
            
            for doc in documents:
                link_tag = doc.css('a')
                title = link_tag.css('::text').get()
                url = link_tag.css('::attr(href)').get()
                
                # Trích xuất ngày từ tiêu đề nếu có (ví dụ: "26/12/2025")
                date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', title) if title else None
                publish_date = date_match.group(1) if date_match else None

                if not title:
                    continue

                summary = title.strip()
                iso_date = convert_date_to_iso8601(publish_date)
                absolute_url = response.urljoin(url)

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