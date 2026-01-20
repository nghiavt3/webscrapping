import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_drc'
    mcpcty = 'DRC'
    allowed_domains = ['drc.com.vn'] 
    start_url = "https://drc.com.vn/wp-admin/admin-ajax.php"

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def start_requests(self):
        # Danh sách các năm bạn muốn lấy dữ liệu
        years = ['2023', '2024', '2025', '2026']
        
        for year in years:
            # Gửi yêu cầu POST với Form Data
            yield scrapy.FormRequest(
                url=self.start_url,
                formdata={
                    'action': 'get_posts_by_year_taxonomy',
                    'year': year
                },
                callback=self.parse_generic,
                meta={'year': year} # Lưu lại năm để đánh dấu dữ liệu
            )
            yield scrapy.FormRequest(
                url=self.start_url,
                formdata={
                    'action': 'get_posts_by_year_taxonomy2',
                    'year': year
                },
                callback=self.parse_generic,
                meta={'year': year} # Lưu lại năm để đánh dấu dữ liệu
            )
            yield scrapy.FormRequest(
                url=self.start_url,
                formdata={
                    'action': 'get_posts_by_year_taxonomy4',
                    'year': year
                },
                callback=self.parse_generic,
                meta={'year': year} # Lưu lại năm để đánh dấu dữ liệu
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

        # Duyệt qua tất cả các item trong mã HTML trả về
        items = response.css('.tin-co-dong_item')
        
        for item in items:
            # 1. Trích xuất tiêu đề (loại bỏ text của thẻ <i> nếu có)
            # Cách này lấy text trực tiếp bên trong thẻ <a> nhưng bỏ qua các thẻ con như <i>
            title = item.css('a ::text').getall()
            title = "".join(title).strip()

            # 2. Trích xuất đường dẫn file PDF/Link
            link = item.css('a::attr(href)').get()

            # 3. Trích xuất ngày đăng
            date = item.css('.tin-co-dong_date::text').get()

            if not title or not date:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso8601(date)
            absolute_url = response.urljoin(link)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{iso_date}".replace(' ', '_').strip()[:150]
            
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
            e_item['details_raw'] = f"{summary}\nLink: {absolute_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None