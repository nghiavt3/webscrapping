import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_cts'
    mcpcty = 'CTS'
    allowed_domains = ['vbse.vn'] 
    start_urls = ['https://www.vbse.vn/danh-muc-co-dong/cong-bo-thong-tin/',
                  'https://www.vbse.vn/danh-muc-co-dong/thong-tin-tai-chinh/'] 

    async def start(self):
        urls = [
            ('https://www.vbse.vn/danh-muc-co-dong/cong-bo-thong-tin/', self.parse),
             ('https://www.vbse.vn/danh-muc-co-dong/thong-tin-tai-chinh/', self.parse),
             ('https://www.vbse.vn/co-dong/danh-sach-dai-hoi-co-dong/', self.parse_dhcd),
             
              
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

        # 2. Lấy tất cả các mục cổ đông
        items = response.css('div.shareholder-item')
        
        for item in items:
            # Trích xuất dữ liệu
            title = item.css('h3.shareholder-title a::text').get()
            url = item.css('h3.shareholder-title a::attr(href)').get()
            excerpt = item.css('p.new-excerpt::text').get()
            date_text = item.css('div.shareholder-item-right p.news-date::text').get()

            if not title:
                continue

            # Làm sạch dữ liệu
            cleaned_title = title.strip()
            cleaned_excerpt = excerpt.strip() if excerpt else ""
            cleaned_date = date_text.strip() if date_text else ""
            iso_date = convert_date_to_iso8601(cleaned_date)
            full_url = response.urljoin(url)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Tạo ID duy nhất (Tiêu đề + Ngày) để tránh trùng lặp và gửi tin rác
            event_id = f"{cleaned_title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item để Pipeline xử lý lưu DB và gửi Telegram
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{cleaned_title}\n{cleaned_excerpt}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

    def parse_dhcd(self, response):
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

        # 2. Lấy tất cả các mục cổ đông
        articles = response.css('article.post')
        
        for art in articles:
            headers = art.css('header.entry-header')
            contents = art.css('div.entry-content')
            for header, content in zip(headers, contents):
                title = header.css('h2.entry-title a::text').get()
                url = header.css('h2.entry-title a::attr(href)').get()
                excerpt = content.css('p::text').get()
                date_text = None

                if not title:
                    continue

                # Làm sạch dữ liệu
                summary = title.strip()
                
                cleaned_excerpt = excerpt.strip() if excerpt else ""
                cleaned_date = date_text.strip() if date_text else ""
                iso_date = convert_date_to_iso8601(cleaned_date)
                full_url = response.urljoin(url)

                # -------------------------------------------------------
                # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
                # -------------------------------------------------------
                # Tạo ID duy nhất (Tiêu đề + Ngày) để tránh trùng lặp và gửi tin rác
                event_id = f"{summary}_{iso_date if iso_date else 'NODATE'}".replace(' ', '_').strip()[:150]
                
                cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
                if cursor.fetchone():
                    self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT.")
                    break 

                # 4. Yield Item để Pipeline xử lý lưu DB và gửi Telegram
                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = summary
                e_item['date'] = iso_date
                e_item['details_raw'] = f"{summary}\n{cleaned_excerpt}\nLink: {full_url}"
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