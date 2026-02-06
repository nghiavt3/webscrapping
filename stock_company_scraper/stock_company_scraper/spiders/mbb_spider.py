import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import json
from scrapy import Selector
from scrapy_playwright.page import PageMethod
class EventSpider(scrapy.Spider):
    name = 'event_mbb'
    mcpcty = 'MBB'
    allowed_domains = ['www.mbbank.com.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def start(self):
        
        year = datetime.now().year
        urls = [
            #(f'https://www.mbbank.com.vn/api/GetListFinance/0/1/{year}',self.parse),
            (f'https://www.mbbank.com.vn/api/GetListFinance/0/1/{year-1}',self.parse),
           # (f'https://www.mbbank.com.vn/api/GetListMessage/1/{year}',self.parse_tbcd),
           # (f'https://www.mbbank.com.vn/api/GetListMessage/1/{year-1}',self.parse_tbcd),
            #('https://www.mbbank.com.vn/api/GetShareHolders/1/1/0',self.parse_dhcd)
            ]
        for url, callback in urls:
            yield scrapy.Request(
                url='https://www.mbbank.com.vn/Investor/dai-hoi-co-dong/0/0//0',
                callback= callback,
                meta={
                "playwright": True,
                # Giả lập trình duyệt thật
                "playwright_context_kwargs": {
                    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                },
                "playwright_page_methods": [
                    # 1. Chờ cho đến khi không còn request mạng nào (dữ liệu đã đổ xong)
                    PageMethod("wait_until", "networkidle"),
                    
                    # 2. Hoặc chờ đích danh một selector của bảng dữ liệu hiện ra
                    # Ví dụ: chờ thẻ <tr> nằm trong danh sách tài liệu
                    #PageMethod("wait_for_selector", ".list-document tr", timeout=10000), 
                    
                    # 3. Cuộn trang xuống dưới cùng để kích hoạt lazy loading (nếu có)
                    PageMethod("evaluate", "window.scrollTo(0, document.body.scrollHeight)"),
                    
                    # 4. Nghỉ thêm 2 giây cho chắc chắn
                    PageMethod("wait_for_timeout", 2000), 
                ],
            },
            )


    async def parse(self, response):
        # 1. Kết nối SQLite
        # conn = sqlite3.connect(self.db_path)
        # cursor = conn.cursor()
        # table_name = f"{self.name}"
        # cursor.execute(f'''
        #     CREATE TABLE IF NOT EXISTS {table_name} (
        #         id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
        #         scraped_at TEXT, web_source TEXT, details_clean TEXT
        #     )
        # ''')

        # 2. Parse JSON an toàn
        # In ra để xem nó là HTML hay JSON
        with open("debug_page.html", "wb") as f:
            f.write(response.body)


        self.logger.info(f"Nội dung phản hồi: {response.text[:100]}") 

        if response.status != 200:
            self.logger.error(f"Lỗi rồi! Status: {response.status}. Không thể parse JSON.")
            return

        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error("Server không trả về JSON hợp lệ. Có thể bạn đã bị chặn hoặc sai URL.")
            return
    
        try:
            data = json.loads(response.text)
            items = data.get('lst', [])
        except Exception as e:
            self.logger.error(f"Lỗi parse JSON: {e}")
            return

        for item in items:
            title = item.get('title')
            link_pdf= 'https://www.mbbank.com.vn' + item.get('file_path')
            raw_date=item.get('last_Save_Date')
            
            
            if not title:
                continue

            iso_date = raw_date[:10]
            full_pdf_url = link_pdf

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # event_id = f"{title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            # cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            # if cursor.fetchone():
            #     self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT.")
            #     # Vì Strapi API trả về list, tin cũ có thể nằm xen kẽ hoặc theo thứ tự, 
            #     # ở đây ta dùng continue thay vì break nếu danh sách không đảm bảo thứ tự thời gian tuyệt đối
            #     continue 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{title}\nLink: {full_pdf_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        # conn.close()

    async def parse_tbcd(self, response):
        # 1. Kết nối SQLite
        # conn = sqlite3.connect(self.db_path)
        # cursor = conn.cursor()
        # table_name = f"{self.name}"
        # cursor.execute(f'''
        #     CREATE TABLE IF NOT EXISTS {table_name} (
        #         id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
        #         scraped_at TEXT, web_source TEXT, details_clean TEXT
        #     )
        # ''')

        # 2. Parse JSON an toàn
        try:
            data = json.loads(response.text)
            all_news = data.get('topNews', []) + data.get('otherNews', [])
        except Exception as e:
            self.logger.error(f"Lỗi parse JSON: {e}")
            return

        for news in all_news:
            title = news.get('title')
            raw_date=news.get('last_save_date')
            fulltext_html = news.get('fulltext', '')
            file_url = ""
            
            if fulltext_html:
                # Tạo một Selector tạm thời từ chuỗi HTML
                sel = Selector(text=fulltext_html)
                # Dùng CSS Selector để tìm thẻ <a> và lấy thuộc tính href
                file_url = sel.css('a::attr(href)').get()
            
            if not title:
                continue

            iso_date = raw_date[:10]
            full_pdf_url = file_url

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # event_id = f"{title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            # cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            # if cursor.fetchone():
            #     self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT.")
            #     # Vì Strapi API trả về list, tin cũ có thể nằm xen kẽ hoặc theo thứ tự, 
            #     # ở đây ta dùng continue thay vì break nếu danh sách không đảm bảo thứ tự thời gian tuyệt đối
            #     continue 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{title}\nLink: {full_pdf_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        # conn.close()

    async def parse_dhcd(self, response):
        # 1. Kết nối SQLite
        # conn = sqlite3.connect(self.db_path)
        # cursor = conn.cursor()
        # table_name = f"{self.name}"
        # cursor.execute(f'''
        #     CREATE TABLE IF NOT EXISTS {table_name} (
        #         id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
        #         scraped_at TEXT, web_source TEXT, details_clean TEXT
        #     )
        # ''')

        # 2. Parse JSON an toàn
        try:
            data = json.loads(response.text)
            items = data.get('lst', [])
        except Exception as e:
            self.logger.error(f"Lỗi parse JSON: {e}")
            return

        for item in items:
            title = item.get('title')
            raw_date=item.get('last_Save_Date')
            fulltext_html = 'https://www.mbbank.com.vn' + item.get('file_path') if item.get('file_path') else None

            
            
            if not title:
                continue

            iso_date = raw_date[:10]
            full_pdf_url = fulltext_html

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # event_id = f"{title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            # cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            # if cursor.fetchone():
            #     self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT.")
            #     # Vì Strapi API trả về list, tin cũ có thể nằm xen kẽ hoặc theo thứ tự, 
            #     # ở đây ta dùng continue thay vì break nếu danh sách không đảm bảo thứ tự thời gian tuyệt đối
            #     continue 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{title}\nLink: {full_pdf_url}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        # conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y - %H:%M')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None