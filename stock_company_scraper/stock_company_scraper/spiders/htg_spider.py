import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_htg'
    mcpcty = 'HTG'
    
    # Tự động lấy năm hiện tại cho URL
    
    allowed_domains = ['hoatho.com.vn'] 
    #start_urls = [f'https://hoatho.com.vn/quan-he-co-dong/thong-tin-co-dong/{current_year}'] 

    async def start(self):
        current_year = datetime.now().year
        urls = [
           # (f'https://hoatho.com.vn/quan-he-co-dong/thong-tin-co-dong/{current_year}', self.parse),
            ('https://hoatho.com.vn/', self.parse_bctc),
             
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
    async def parse_bctc(self, response):
        finance_url = response.css('nav#autoClone-NavMenu a[title="Tình hình tài chính"]::attr(href)').get()
        dhcd_url = response.css('nav#autoClone-NavMenu a[title="Đại hội đồng cổ đông"]::attr(href)').get()
        ttcd_url = response.css('nav#autoClone-NavMenu a[title="Thông tin cổ đông"]::attr(href)').get()
        if finance_url:
            yield scrapy.Request(
                url=response.urljoin(finance_url), 
                callback=self.parse
            )
        if dhcd_url:
            yield scrapy.Request(
                url=response.urljoin(dhcd_url), 
                callback=self.parse
            )
        if ttcd_url:
            yield scrapy.Request(
                url=response.urljoin(ttcd_url), 
                callback=self.parse
            )    
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

        # 2. Selector chính: Các khối tài liệu PDF
        document_items = response.css('div.item-pdf')
        
        for item in document_items:
            date_raw = item.css('div.date::text').get()
            link_selector = item.css('div.title a')
            title_raw = link_selector.css('::text').get()
            relative_url = link_selector.css('::attr(href)').get()

            if not title_raw:
                continue

            title = title_raw.strip()
            iso_date = convert_date_to_iso8601(date_raw)
            full_url = response.urljoin(relative_url)

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