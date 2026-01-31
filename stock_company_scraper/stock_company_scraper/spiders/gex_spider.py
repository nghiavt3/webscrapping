import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_gex'
    mcpcty = 'GEX'
    allowed_domains = ['gelex.vn'] 
   
    async def start(self):
        start_urls = [
            ('https://gelex.vn/doc-cat/cong-bo-thong-tin-2',self.parse ),
            ('https://gelex.vn/doc-cat/bao-cao-tai-chinh',self.parse_bctc )
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
        reports = response.css('.ul-report-list li.li-report-list')
        for report in reports:
            pdf_link = report.css('.li-report-item-title-link::attr(href)').get()
            date_str = report.css('.meta::text').get()
            title_raw = report.css('.li-report-item-title-link::text').get()
            
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
        # 1. Lấy năm đang được chọn (active)
        # Class 'active' nằm ở thẻ <a> hoặc lấy data_year ở thẻ <li> cha
        current_year = response.css('.nav-year-item a.active::text').get()
        if not current_year:
            current_year = response.css('.nav-year-item.active::attr(data_year)').get()
            
        # 2. Duyệt qua từng dòng (row) trong bảng dữ liệu
        # Chúng ta tập trung vào các dòng có class 'child' vì đó là dòng chứa dữ liệu thực tế
        rows = response.css('table tbody tr.child')
        
        for row in rows:
            # Lấy tên loại báo cáo (ví dụ: Báo Cáo Riêng, Báo Cáo Hợp Nhất)
            report_type = row.css('td.quatar::text').get()
            if report_type:
                report_type = report_type.strip()

            # 3. Duyệt qua 4 cột tương ứng với Q1, Q2, Q3, Q4
            quarters = row.css('td.quarter')
            
            for index, q_cell in enumerate(quarters):
                quarter_label = f"Q{index + 1}"
                
                # Kiểm tra nếu ô này có file PDF
                pdf_link = q_cell.css('a::attr(href)').get()
                publish_date = q_cell.css('.date-pdf::text').get()

                if pdf_link:
                    
                    iso_Date= convert_date_to_iso8601(publish_date)
                    full_url = response.urljoin(pdf_link)
                    e_item = EventItem()
                    e_item['mcp'] = self.mcpcty
                    e_item['web_source'] = self.allowed_domains[0]
                    e_item['summary'] = f"{current_year}-{report_type}-{quarter_label}"
                    e_item['date'] = iso_Date
                    e_item['details_raw'] = f"Link: {full_url}"
                    e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    yield e_item
        url_2nd = response.css('.nav-year-item:nth-child(2) a::attr(href)').get()
        full_url_2nd = response.urljoin(url_2nd)
        if full_url_2nd != response.url:
            yield scrapy.Request(
                    url=full_url_2nd,
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