import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_vsh'
    mcpcty = 'VSH'
    allowed_domains = ['vshpc.evn.com.vn'] 
    start_urls = ['https://vshpc.evn.com.vn/c2/vi-VN/news-tl/Quan-he-co-dong-9'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def parse(self, response):
        # 1. Khởi tạo SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''DROP TABLE IF EXISTS {table_name}''')
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')
        items = response.css('.npt-document-list li')
        for item in items:
            date_raw = item.css('.time-day::text').get(default='').strip()
            title_raw = item.css('h6.title::text').get(default='').strip()
            url = item.css('a.title-document::attr(href)').get()
            full_url = response.urljoin(url)
            if not date_raw or not title_raw:
                continue

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

            e_item['details_raw'] = f"Link:\n{full_url}"
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
        container = response.css('#ContentPlaceHolder1_ctl00_3864_FullDescirbe')
        pdf_links = container.css('a')
        files = []
        for link in pdf_links:
            relative_url = link.css('::attr(href)').get()
            file_name = link.css('::text').get()
            
            if relative_url:
                files.append({
                    'file_name': file_name.strip() if file_name else "Untitled",
                    'file_url': response.urljoin(relative_url) # Đảm bảo link tuyệt đối
                })
        
        # 2. Gán danh sách file vào item
        item['details_raw'] = f"{item['details_raw']}/n pdf:{files}"
        
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