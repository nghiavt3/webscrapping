import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_vos'
    mcpcty = 'VOS'
    allowed_domains = ['vosco.vn','finance.vietstock.vn'] 
    #start_urls = ['https://www.vosco.vn/vi/a/tin-tuc-co-dong-129'] 

    async def start(self):
        urls = [
            ('https://www.vosco.vn/vi/a/tin-tuc-co-dong-129', self.parse),
            ('https://finance.vietstock.vn/VOS-ctcp-van-tai-bien-viet-nam.htm', self.parse_bctc), 
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

        # 2. Duyệt qua từng bài viết
        articles = response.css('article.hentry__2')
        
        for article in articles:
            title = article.css('.hentry__title a::text').get()
            url = article.css('.hentry__title a::attr(href)').get()
            # Lấy datetime gốc từ thẻ time (thường là YYYY-MM-DD)
            date_machine = article.css('time.date::attr(datetime)').get()
            
            if not title or not date_machine:
                continue

            summary = title.strip()
            # Đảm bảo định dạng chỉ lấy YYYY-MM-DD nếu chuỗi dài hơn
            iso_date = date_machine[:10] 

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT.")
                break 

            # 4. Trích xuất file đính kèm
            file_links = []
            for link in article.css('span.news-file a'):
                f_url = link.css('::attr(href)').get()
                f_title = link.css('::attr(title)').get() or "File đính kèm"
                if f_url:
                    file_links.append(f"{f_title}: {response.urljoin(f_url)}")

            # 5. Yield Item
            item = EventItem()
            item['mcp'] = self.mcpcty
            item['web_source'] = self.allowed_domains[0]
            item['summary'] = summary
            item['date'] = iso_date
            
            full_article_url = response.urljoin(url)
            item['details_raw'] = f"{summary}\nURL: {full_article_url}\nFiles: {', '.join(file_links)}"
            item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield item

        conn.close()

    def parse_bctc(self, response):
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

        # 2. Duyệt qua từng bài viết
        rows = response.css('div.table-responsive table tr')
        
        for row in rows:
            link_node = row.css('a.news-link')
            title = link_node.css('::text').get().strip() if link_node else None
            url = link_node.css('::attr(href)').get()
            # Lấy datetime gốc từ thẻ time (thường là YYYY-MM-DD)
            raw_date = row.css('td.col-date::text').get()
            
            if not title or not raw_date:
                continue

            summary = title.strip()
            # Đảm bảo định dạng chỉ lấy YYYY-MM-DD nếu chuỗi dài hơn
            iso_date = convert_to_isodate(raw_date)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT.")
                break 


            # 5. Yield Item
            item = EventItem()
            item['mcp'] = self.mcpcty
            item['web_source'] = self.allowed_domains[1]
            item['summary'] = summary
            item['date'] = iso_date
            
            full_article_url = response.urljoin(url)
            item['details_raw'] = f"{summary}\nURL: {full_article_url}"
            item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield item

        conn.close()    
def convert_to_isodate(date_str):
    if not date_str:
        return None
    try:
        # Định dạng đầu vào dd/mm/yyyy
        date_obj = datetime.strptime(date_str.strip(), '%d/%m/%Y')
        # Trả về định dạng ISO YYYY-MM-DD
        return date_obj.strftime('%Y-%m-%d')
    except ValueError:
        return None