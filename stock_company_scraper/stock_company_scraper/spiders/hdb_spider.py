import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_hdb'
    mcpcty = 'HDB'
    allowed_domains = ['hdbank.com.vn'] 
    start_urls = ['https://hdbank.com.vn/vi/investor/thong-tin-nha-dau-tu/quan-he-co-dong/cong-bo-thong-tin-thong-tin-khac'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        yield scrapy.Request(
            url=self.start_urls[0],
            callback=self.parse,
            meta={'playwright': True}
        )
    
    def parse(self, response):
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

        # 2. Lấy tất cả các thẻ <p> trong vùng content
        all_paragraphs = response.css('div.content-tab__content div p')
        
        # Lặp qua các thẻ <p> theo nhóm 2 (Thông báo + Ngày/Giờ)
        for i in range(0, len(all_paragraphs), 2):
            if i + 1 < len(all_paragraphs):
                announcement_p = all_paragraphs[i]
                datetime_p = all_paragraphs[i+1]
                
                # Trích xuất Tiêu đề và Link
                link_selector = announcement_p.css('a')
                title_raw = link_selector.css('::text').get()
                relative_url = link_selector.css('::attr(href)').get()
                
                if not title_raw:
                    continue

                title = title_raw.strip()
                url = response.urljoin(relative_url) if relative_url else ""
                
                # Trích xuất ngày giờ
                date_time_raw = datetime_p.css('::text').getall()
                pub_date_str = ' '.join([t.strip() for t in date_time_raw if t.strip()])
                iso_date = convert_date_to_iso8601(pub_date_str)

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
                e_item['details_raw'] = f"{title}\nLink: {url}"
                e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    # Định dạng HDBank: "Ngày 02/07/2025 vào lúc 11:05:34"
    input_format = 'Ngày %d/%m/%Y vào lúc %H:%M:%S'
    output_format = '%Y-%m-%d'
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        return date_object.strftime(output_format)
    except ValueError:
        # Fallback nếu định dạng ngày bị thay đổi nhẹ
        return None