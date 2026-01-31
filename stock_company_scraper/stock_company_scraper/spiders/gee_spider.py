import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_gee'
    mcpcty = 'GEE'
    allowed_domains = ['gelex-electric.com'] 
   
    async def start(self):
        start_urls = [
            ('https://gelex-electric.com/doc-cat/cong-bo-thong-tin-2',self.parse ),
            ('https://gelex-electric.com/doc-cat/bao-cao-tai-chinh',self.parse_bctc )
        ]
        for url, callback in start_urls: 
            yield scrapy.Request(
                url=url,
                callback=callback,
                #meta={'playwright': True}
            )
        
    
    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def parse(self, response):
        # 1. Kết nối SQLite và chuẩn bị bảng
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
        items = response.css('.report-items .report-item')
        for item in items:
            pdf_link = item.css('.report-item-link a::attr(href)').get()
            date_str = item.css('.entry-date::text').get()
            title_raw = item.css('.title.heading_3 a::text').get()
            
            # Xử lý làm sạch chuỗi (loại bỏ khoảng trắng thừa ở đầu/cuối)
            if date_str:
                date_str = date_str.strip()
            if title_raw:
                title_raw = title_raw.strip()
            if not title_raw:
                continue

            summary = title_raw.strip()
            iso_date = convert_date_to_iso8601(date_str)
            # Khải Hoàn Land thường dùng link tuyệt đối, nhưng urljoin vẫn an toàn hơn
            full_pdf_url = response.urljoin(pdf_link)

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
            e_item['details_raw'] = f"Link: {full_pdf_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()
    
    async def parse_bctc(self, response):
        # Duyệt qua các hàng có class 'child'
        year_active = response.css('.nav-year a.active::text').get()
        rows = response.css('table.table-report tr.child')
        
        for row in rows:
            # Lấy tên loại báo cáo (ví dụ: Báo Cáo Riêng)
            report_type = row.css('td.quatar::text').get()
            if report_type:
                report_type = report_type.strip()

            # Lấy tất cả các ô chứa dữ liệu quý (tổng cộng 4 ô)
            quarters = row.css('td.quarter')
            
            for index, td in enumerate(quarters):
                quarter_name = f"Q{index + 1}" # Index 0 là Q1, 1 là Q2...
                
                # Kiểm tra xem trong ô có thẻ <a> (link PDF) không
                link_node = td.css('a')
                if link_node:
                    pdf_url = link_node.css('::attr(href)').get()
                    date = td.css('.meta-date::text').get()
                    
                    
                    iso_Date= convert_date_to_iso8601(date.strip() if date else None)
                    full_url = response.urljoin(pdf_url)
                    e_item = EventItem()
                    e_item['mcp'] = self.mcpcty
                    e_item['web_source'] = self.allowed_domains[0]
                    e_item['summary'] = f"{year_active}-{report_type}-{quarter_name}"
                    e_item['date'] = iso_Date
                    e_item['details_raw'] = f"Link: {full_url}"
                    e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    yield e_item
        url_2nd = response.css('.nav-year-item:nth-child(2) a::attr(href)').get()
        if url_2nd != response.url:
            yield scrapy.Request(
                    url=url_2nd,
                    callback=self.parse_bctc,
                )
        
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