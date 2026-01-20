import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_dsc'
    mcpcty = 'DSC'
    allowed_domains = ['dsc.com.vn'] 
    # Danh sách các mục cần quét của DSC
    start_urls = [
        'https://www.dsc.com.vn/quan-he-co-dong/cong-bo-thong-tin',
        'https://www.dsc.com.vn/quan-he-co-dong/thong-tin-co-phieu',
        'https://www.dsc.com.vn/quan-he-co-dong/dai-hoi-dong-co-dong',
        'https://www.dsc.com.vn/quan-he-co-dong/thong-tin-tai-chinh'
    ]

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                # Kích hoạt Playwright để render nội dung động
                meta={"playwright": True}
            )

    def parse(self, response):
        # 1. Kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Chọn các khối tin tức (Sử dụng selector chứa cụm từ để ổn định)
        items = response.css('div[class*="ItemInformation_scdtqwxkwn"]')

        for item in items:
            title = (item.css('h3::text').get() or "").strip()
            date_raw = (item.css('span[class*="ItemInformation_xgvxdfaaul"]::text').get() or "").strip()
            pdf_link = item.css('a::attr(href)').get()
            
            if not title:
                continue

            iso_date = convert_date_to_iso8601(date_raw)
            full_url = response.urljoin(pdf_link)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ TẠI {response.url}: [{title}]. BỎ QUA.")
                continue # Với đa URL, ta dùng continue thay vì break để check các mục khác

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