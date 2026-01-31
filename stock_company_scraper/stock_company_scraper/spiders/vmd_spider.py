import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_vmd'
    mcpcty = 'VMD' 
    allowed_domains = ['vietpharm.com.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    # URL đích của admin-ajax
    ajax_url = 'https://vietpharm.com.vn/wp-admin/admin-ajax.php'

    def start_requests(self):
        # Payload copy từ yêu cầu của bạn
        payload = {
            'action': 'bt_bb_get_grid',
            'number': '1000',
            'category': '',
            'show': '{"category":false,"date":false,"author":false,"comments":true,"excerpt":true,"share":false}',
            'bt-bb-masonry-post-grid-nonce': 'a6951cf79d', # Lưu ý: Nonce này có thể hết hạn theo thời gian
            'post-type': 'post',
            'offset': '0'
        }

        # Headers quan trọng
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Referer': 'https://vietpharm.com.vn/blog/ban-tin-vimedimex/'
        }

        yield scrapy.FormRequest(
            url=self.ajax_url,
            formdata=payload,
            headers=headers,
            callback=self.parse
        )

    def parse(self, response):
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
        # Chọn tất cả các khối item bài viết
        items = response.css('div.bt_bb_grid_item')
        
        for item in items:     
            # 1. Trích xuất Tiêu đề
            title = item.css('h5.bt_bb_grid_item_post_title a::text').get()
            
            # 2. Trích xuất Link bài viết
            link = item.css('h5.bt_bb_grid_item_post_title a::attr(href)').get()
            
            # 3. Trích xuất Link ảnh thumbnail (nằm trong attribute data-src)
            image_url = item.css('::attr(data-src)').get()
            
            # 4. Trích xuất mô tả ngắn (excerpt)
            excerpt = item.css('div.bt_bb_grid_item_post_excerpt::text').get()
            
            # 5. Trích xuất dữ liệu ẩn bên trong (nếu có nội dung bảng hoặc văn bản bổ sung)
            # Trong file bạn gửi có đoạn chứa thông tin nhân sự/phòng khám
            extra_content = item.css('div.bt_bb_grid_item_post_content ::text').getall()
            full_text = " ".join([t.strip() for t in extra_content if t.strip()])
            if not title:
                continue

            summary = title.strip()
            iso_date = None
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
            e_item['details_raw'] = f"{excerpt}\nLink: {absolute_url} \n"
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