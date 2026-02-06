import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_cii'
    mcpcty = 'CII'
    allowed_domains = ['cii.com.vn'] 
    start_urls = ['https://cii.com.vn/category/thong-tin-cong-bo'] 

    async def start(self):
        urls = [
            ('https://cii.com.vn/category/thong-tin-cong-bo', self.parse),
            ('https://cii.com.vn/ket-qua-san-xuat-kinh-doanh/bao-cao-tai-chinh', self.parse_bctc),
            ('https://cii.com.vn/ket-qua-san-xuat-kinh-doanh/bao-cao-tai-chinh-hop-nhat', self.parse_bctc),
        ]
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                callback=callback,
               # meta={'playwright': True}
            )

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # 1. Kết nối SQLite
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

        # 2. Duyệt qua các bài đăng
        posts = response.css('div.post_item')
        current_year = str(datetime.now().year)

        for item in posts:
            # Trích xuất dữ liệu thô
            date_raw = item.css('div.date_post span::text').get()
            title = item.css('div.title_post h3 a::text').get()
            detail_url = item.css('div.title_post h3 a::attr(href)').get()
            summary = item.css('div.excerpt_post p span::text').get()
            
            # Làm sạch và định dạng
            cleaned_title = title.strip() if title else ""
            iso_date = convert_viet_date_to_iso8601(date_raw, current_year)
            full_url = response.urljoin(detail_url)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{cleaned_title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                break 

            # 4. Đóng gói Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{cleaned_title}\nSummary: {summary}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()
    
    def parse_bctc(self, response):
        # 1. Kết nối SQLite
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

        # 2. Duyệt qua các bài đăng
        tabs = response.css('div.wpb_tab')
        for tab in tabs:
            reports = tab.css('div.wpb_text_column')
            for report in reports:
                link_node = report.css('h4 a')
                title = link_node.css('::text').get()
                url = link_node.css('::attr(href)').get()
                # Làm sạch và định dạng
                cleaned_title = title.strip().replace('\xa0', ' ')
                iso_date = None
                full_url = response.urljoin(url)

                # -------------------------------------------------------
                # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
                # -------------------------------------------------------
                event_id = f"{cleaned_title}_{iso_date}".replace(' ', '_').strip()[:150]
                
                cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
                if cursor.fetchone():
                    self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                    break 

                # 4. Đóng gói Item
                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = cleaned_title
                e_item['date'] = iso_date
                e_item['details_raw'] = f"{cleaned_title}\nLink: {full_url}"
                e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                yield e_item

        conn.close()

# Mapping tháng tiếng Việt
MONTH_MAPPING = {
    'Tháng 01': '01', 'Tháng 1': '01', 'Tháng 02': '02', 'Tháng 2': '02',
    'Tháng 03': '03', 'Tháng 3': '03', 'Tháng 04': '04', 'Tháng 4': '04',
    'Tháng 05': '05', 'Tháng 5': '05', 'Tháng 06': '06', 'Tháng 6': '06',
    'Tháng 07': '07', 'Tháng 7': '07', 'Tháng 08': '08', 'Tháng 8': '08',
    'Tháng 09': '09', 'Tháng 9': '09', 'Tháng 10': '10', 'Tháng 11': '11', 'Tháng 12': '12'
}

def convert_viet_date_to_iso8601(vietnam_day_month_str, year):
    if not vietnam_day_month_str: return None
    
    parts = vietnam_day_month_str.strip().split(maxsplit=1)
    if len(parts) < 2: return None
    
    day = parts[0].zfill(2)
    month = MONTH_MAPPING.get(parts[1].strip())
    
    if not month: return None
    
    return f"{year}-{month}-{day}"