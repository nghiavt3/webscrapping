import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_vib'
    mcpcty = 'VIB'
    allowed_domains = ['vib.com.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        # Duyệt song song cả hai nguồn tin tức quan trọng của VIB
        urls = [
            ('https://www.vib.com.vn/vn/nha-dau-tu/cong-bo-thong-tin', self.parse_cong_bo),
            ('https://www.vib.com.vn/vn/nha-dau-tu/thong-tin-co-dong', self.parse_co_dong),
        ]
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                callback=callback, 
                meta={'playwright': True}
            )

    def parse_common(self, response, source_name):
        """Hàm dùng chung để trích xuất dữ liệu từ cấu trúc h4 của VIB"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        posts = response.css('.vib-v2-report-tab-list-detail h4')
        
        for post in posts:
            title = (post.css('a::text').get() or "").strip()
            link = post.css('a::attr(href)').get()
            raw_date = post.css('i::text').get()
            
            if not title or not raw_date:
                continue

            iso_date = convert_date_to_iso8601(raw_date)
            summary = title

            # -------------------------------------------------------
            # KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ TẠI {source_name}: [{summary}]. DỪNG.")
                break 

            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['date'] = iso_date
            e_item['summary'] = summary
            
            full_url = response.urljoin(link) if link else "N/A"
            e_item['details_raw'] = f"{summary}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

    def parse_cong_bo(self, response):
        yield from self.parse_common(response, "CÔNG BỐ THÔNG TIN")

    def parse_co_dong(self, response):
        yield from self.parse_common(response, "THÔNG TIN CỔ ĐÔNG")

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    # VIB thường có định dạng DD/MM/YYYY HH:MM trong thẻ <i>
    input_format = '%d/%m/%Y %H:%M'
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), input_format)
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        # Backup nếu chỉ có ngày
        try:
            return datetime.strptime(vietnam_date_str.strip()[:10], '%d/%m/%Y').strftime('%Y-%m-%d')
        except:
            return None