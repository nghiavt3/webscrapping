import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_dbc'
    mcpcty = 'DBC' 
    allowed_domains = ['dabaco.com.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def start(self):
        urls = [
            ('http://dabaco.com.vn/vn/thong-tin-chung.html', self.parse_generic),
             ('http://dabaco.com.vn/vn/thong-tin-tai-chinh.html', self.parse_generic),
             
            
        ]
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                callback=callback,
                #meta={'playwright': True}
            )

    def parse_generic(self, response):
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
        items = response.css('#right_type div[style*="padding:10px"]')
        
        for item in items:          
            title = item.css('a::text').get(default='').strip()
            date = item.css('span::text').get()
            link = item.css('a::attr(href)').get()
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
            if absolute_url  and "báo cáo tài chính" in summary.lower():
                yield scrapy.Request(
                    url=absolute_url,
                    callback=self.parse_detail,
                    meta={'item': e_item}  # Chuyển dữ liệu sang hàm tiếp theo
                )
            else:
                yield e_item   

        conn.close()
    
    def parse_detail(self, response):
        # Nhận lại item từ trang danh sách gửi qua meta
        item = response.meta['item']
        
        # 1. Trích xuất tất cả các link PDF trong khối chi tiết
        # Chúng ta tìm các thẻ <a> nằm trong .blog-details-col có href chứa ".pdf"
        links = response.css('p a::attr(href)').getall()

        # Nếu bạn muốn lấy cả Text và URL để biết link đó là báo cáo gì:
        for linka in response.css('p a'):
            url = linka.css('::attr(href)').get()
            # Lấy toàn bộ text bên trong các thẻ span, i (nếu có)
            title = "".join(linka.css('::text, span::text, i::text').getall()).strip()
            item['details_raw'] = f"{item['details_raw']}\n Tiêu đề:{title}\nLink:{response.urljoin(url)}"
        yield item

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None