import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_st8'
    mcpcty = 'ST8' 
    allowed_domains = ['st8.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def start(self):
        urls = [
            #('https://st8.vn/quan-he-co-dong/dai-hoi-co-dong', self.parse_generic),
             #('https://st8.vn/quan-he-co-dong/cong-bo-thong-tin', self.parse_cbtt),
             ('https://st8.vn/quan-he-co-dong/bao-cao-tai-chinh?year=2025', self.parse),
            
             
            
        ]
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                callback=callback,
                #meta={'playwright': True}
            )

    async def parse_generic(self, response):
        """Hàm parse dùng chung cho các chuyên mục của SeABank"""
        # 1. Khởi tạo SQLite
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
        # Lấy tất cả các hàng trừ hàng tiêu đề năm
        rows = response.css('.financial-report tbody tr')
        
        for row in rows:            
            title = row.css('td:first-child b::text').get()
            date = row.css('td:last-child span::text').get()
            link = row.css('a.financial-report__file::attr(href)').get()
            
            if not title:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso8601(date)
            absolute_url = f"{response.urljoin(link)}"

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{iso_date if iso_date else 'NODATE'}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT CHUYÊN MỤC.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{summary}\nLink: {absolute_url} \n"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

    async def parse_cbtt(self, response):
        """Hàm parse dùng chung cho các chuyên mục của SeABank"""
        # 1. Khởi tạo SQLite
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
        # Lấy tất cả các hàng trừ hàng tiêu đề năm
        items = response.css('.public-info__item')
        
        for item in items:       
            title = item.css('.public-info__item__title a::text').get(default='').strip()
            date = item.css('.public-info__item__time::text').get()
            date_part = date.strip().split(' ')[0]
            link = item.css('.public-info__item__title a::attr(href)').get()
            
            if not title:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso8601(date_part)
            absolute_url = f"{response.urljoin(link)}"

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{iso_date if iso_date else 'NODATE'}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT CHUYÊN MỤC.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{summary}\nLink: {absolute_url} \n"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

    async def parse(self, response):
        # 1. Lấy danh sách các Quý từ Header của bảng làm bản đồ cột
        quarters = response.css('.financial-report thead th[scope="col"]::text').getall()
        quarters = [q.strip() for q in quarters] # Kết quả: ['Quý I', 'Quý II / Bán niên', 'Quý III', 'Quý IV / Cả năm']

        # 2. Duyệt qua từng hàng trong tbody
        current_category = ""
        rows = response.css('.financial-report tbody tr')

        for row in rows:
            # Kiểm tra nếu hàng là tiêu đề nhóm (BCTC Kiểm toán hoặc Tự lập)
            category_check = row.css('td[class*="--bg-color"] b::text').get()
            if category_check:
                current_category = category_check.strip()
                continue

            # Lấy tên loại báo cáo (Cột 1)
            report_type = row.css('td:first-child::text').get()
            if report_type:
                report_type = report_type.strip()

                # Lấy tất cả các ô dữ liệu (td) trừ ô đầu tiên
                cells = row.css('td:not(:first-child)')

                for index, cell in enumerate(cells):
                    # Tìm thẻ <a> trong ô
                    link_node = cell.css('a.financial-report__file')
                    
                    if link_node:
                        # yield {
                        #     'category': current_category,
                        #     'report_type': report_type,
                        #     'quarter': quarters[index],
                        #     'file_url': link_node.css('::attr(href)').get(),
                        #     'publish_date': link_node.css('span::text').get(),
                        #     'file_ext': 'PDF'
                        # }
                        summary = f"{current_category}-{report_type}-{quarters[index]}"
                        iso_date = convert_date_to_iso8601(link_node.css('span::text').get())
                        absolute_url = f"{response.urljoin(link_node.css('::attr(href)').get())}"

                        # 4. Yield Item
                        e_item = EventItem()
                        e_item['mcp'] = self.mcpcty
                        e_item['web_source'] = self.allowed_domains[0]
                        e_item['summary'] = summary
                        e_item['date'] = iso_date
                        e_item['details_raw'] = f"{summary}\nLink: {absolute_url} \n"
                        e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        
                        yield e_item

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None