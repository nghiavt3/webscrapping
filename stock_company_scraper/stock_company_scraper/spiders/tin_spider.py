import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_tin'
    mcpcty = 'TIN'
    allowed_domains = ['vietcredit.com.vn'] 
    start_urls = ['https://www.vietcredit.com.vn/nha-dau-tu/thong-bao-co-dong-ban-tin/'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def parse(self, response):
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
        for blog in response.css('.blog-col'):
            date_raw = blog.css('.blog-info::text').get(default='').strip()
            title_raw = blog.css('.blog-content h4 a::text').get()
            url = blog.css('.blog-content h4 a::attr(href)').get()
            excerpt = "".join(blog.css('.blog-content p::text').getall()).strip()
            if not date_raw or not title_raw:
                continue
            full_url = response.urljoin(url)    
            summary = title_raw.strip()
            iso_date = clean_vietnamese_date(date_raw.strip())
            
            
            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{summary}_{iso_date if iso_date else 'NODATE'}".replace(' ', '_').strip()[:150]
            
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

            e_item['details_raw'] = f"{excerpt}\nLink:\n{full_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if full_url  and "báo cáo tài chính" in summary.lower():
                yield scrapy.Request(
                    url=full_url,
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
        pdf_elements = response.css('.blog-details-col a[href$=".pdf"]')
        
        files = []
        for anchor in pdf_elements:
            file_name = anchor.css('::text').get()
            file_url = anchor.css('::attr(href)').get()
            
            if file_url:
                files.append({
                    'file_name': file_name.strip() if file_name else "Untitled",
                    'file_url': response.urljoin(file_url) # Đảm bảo link tuyệt đối
                })
                item['details_raw'] = f"{item['details_raw']}\n {response.urljoin(file_url)}"
        
        # 2. Gán danh sách file vào item
        #item['details_raw'] = f"{item['details_raw']}/n pdf:{files}"
        
        # 3. Trả về item hoàn chỉnh cho Pipeline
        yield item
        
def clean_vietnamese_date(date_str):
    if not date_str:
        return None
    
    # Từ điển ánh xạ tên tháng tiếng Việt sang số
    month_map = {
        "Tháng Một": "01", "Tháng Hai": "02", "Tháng Ba": "03",
        "Tháng Tư": "04", "Tháng Năm": "05", "Tháng Sáu": "06",
        "Tháng Bảy": "07", "Tháng Tám": "08", "Tháng Chín": "09",
        "Tháng Mười Một": "11", "Tháng Mười Hai": "12", "Tháng Mười": "10" 
    }
    
    # Lưu ý: "Tháng Mười Một" và "Tháng Mười Hai" phải được kiểm tra trước "Tháng Mười" 
    # để tránh việc replace nhầm. Cách an toàn nhất là split chuỗi.
    
    try:
        # Tách chuỗi: "29", "Tháng", "Một,", "2026"
        parts = date_str.replace(',', '').split()
        day = parts[0].zfill(2) # Đảm bảo có 2 chữ số
        year = parts[-1]
        
        # Lấy phần tên tháng (ví dụ: "Tháng Một")
        month_name = " ".join(parts[1:-1])
        month = month_map.get(month_name, "01")
        
        return f"{year}-{month}-{day}"
    except:
        return date_str # Trả về nguyên bản nếu có lỗi định dạng