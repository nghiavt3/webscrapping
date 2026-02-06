import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_dvc'
    mcpcty = 'DVC'
    allowed_domains = ['dichvucang.com','finance.vietstock.vn'] 
    

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def start(self):
        
        yield scrapy.Request(
                url='https://www.dichvucang.com/default.aspx?pageid=news&cate=3', 
                callback=self.parse,
                meta={'playwright': True}
            )
        yield scrapy.Request(
                url='https://finance.vietstock.vn/DVC-ctcp-thuong-mai-dich-vu-tong-hop-cang-hai-phong.htm', 
                callback=self.parse_bctc,
                #meta={'playwright': True}
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

        # 2. Lặp qua từng mục tin (td)
        news_items = response.css('#Home1_ctl08_dlServices > tbody > tr > td')

        for item in news_items:
            title_anchor = item.css('.nd_center a')
            title = title_anchor.css('::text').get()
            url = title_anchor.css('::attr(href)').get()
            date_raw = item.css('.noidung::text').get()
            if date_raw:
                date_raw = date_raw
            else :
                date_raw = item.css('.noidung i::text').get()
            if not title:
                continue

            # 3. Xử lý logic ngày tháng đặc thù: "Tin ngày [30/12/2025 10:00]"
            pub_date_str = ""
            if date_raw:
                start = date_raw.find('[')
                end = date_raw.find(']')
                if start != -1 and end != -1:
                    pub_date_str = date_raw[start+1:end]
                else:
                    pub_date_str = date_raw.replace('Tin ngày', '').strip()

            iso_date = convert_date_to_iso8601(pub_date_str)
            full_url = response.urljoin(url)
            cleaned_title = title.strip()

            # -------------------------------------------------------
            # 4. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{cleaned_title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                break 

            # 5. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{cleaned_title}\nLink: {full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

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
            
def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    
    # Hỗ trợ cả trường hợp có giờ và không có giờ
    formats = ['%d/%m/%Y %H:%M', '%d/%m/%Y']
    output_format = '%Y-%m-%d'

    for fmt in formats:
        try:
            date_object = datetime.strptime(vietnam_date_str.strip(), fmt)
            return date_object.strftime(output_format)
        except ValueError:
            continue
    return None