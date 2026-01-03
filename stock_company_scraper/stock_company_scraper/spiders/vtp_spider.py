import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_vtp'
    mcpcty = 'VTP'
    allowed_domains = ['viettelpost.com.vn'] 
    start_urls = ['https://viettelpost.com.vn/tin-co-dong/'] 

    # Cấu hình tối ưu cho Playwright và tránh bị chặn
    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'DOWNLOAD_DELAY': 2,
    }

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

        # 2. Lấy tất cả các khối tin bài (hỗ trợ cả 3 loại item của VTP)
        records = response.css('div.first-item, div.second-item, div.normal-item')
        
        for record in records:
            title = record.css('h5::text, p.title::text').get()
            description = record.css('p.des::text, p.description::text').get()
            article_url = record.css('a::attr(href)').get()
            
            # Xử lý ngày tháng từ nhiều node span hoặc p
            date_nodes = record.css('div.meta span::text, p.date::text').getall()
            date_raw = " ".join(date_nodes).replace('\xa0', ' ').strip()
            
            if not title or not date_raw:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso(date_raw)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
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
            
            full_url = response.urljoin(article_url) if article_url else "N/A"
            clean_desc = description.strip() if description else ""
            e_item['details_raw'] = f"{summary}\n{clean_desc}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

# Bổ sung logic ánh xạ tháng cho hàm convert
THANG_MAPPING = {
    'tháng 1': 1, 'tháng 2': 2, 'tháng 3': 3, 'tháng 4': 4,
    'tháng 5': 5, 'tháng 6': 6, 'tháng 7': 7, 'tháng 8': 8,
    'tháng 9': 9, 'tháng 10': 10, 'tháng 11': 11, 'tháng 12': 12,
}

def convert_date_to_iso(date_str):
    if not date_str: return None
    date_str = date_str.lower().strip()
    
    # Định dạng DD/MM/YY
    if '/' in date_str:
        try:
            return datetime.strptime(date_str, '%d/%m/%y').strftime('%Y-%m-%d')
        except: pass

    # Định dạng "15 Tháng 9, 2025"
    if 'tháng' in date_str:
        try:
            # Tách chuỗi để lấy ngày, tháng, năm
            parts = date_str.replace(',', '').split()
            # [15, tháng, 9, 2025]
            day = int(parts[0])
            month_val = THANG_MAPPING.get(f"tháng {parts[2]}") or int(parts[2])
            year = int(parts[3])
            return datetime(year, month_val, day).strftime('%Y-%m-%d')
        except: return None
    return None