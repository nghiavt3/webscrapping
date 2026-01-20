import scrapy
import sqlite3
import os
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_bvh'
    mcpcty = 'BVH'
    allowed_domains = ['baoviet.com.vn'] 
    start_urls = ['https://baoviet.com.vn/vi/quan-he-co-dong'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # 1. Kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY,
                mcp TEXT,
                date TEXT,
                summary TEXT,
                scraped_at TEXT,
                web_source TEXT,
                details_clean TEXT
            )
        ''')

        # 2. Duyệt qua từng nhóm Accordion (thường chia theo Quý/Năm)
        for quarter_group in response.css('.item.accordion'):
            # Duyệt qua từng bài đăng (panel) trong nhóm đó
            panels = quarter_group.css('.f-panel')
            
            for panel in panels:
                post_title = (panel.css('.post__title::text').get() or "").strip()
                post_date_raw = (panel.css('.post__date time::text').get() or "").strip()
                
                # Chuyển đổi ngày (BVH dùng dấu chấm: 29.12.2025)
                iso_date = convert_date_to_iso8601(post_date_raw)

                # -------------------------------------------------------
                # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
                # -------------------------------------------------------
                event_id = f"{post_title}_{iso_date}".replace('/', '-').replace('.', '_').strip()[:150]
                
                cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
                if cursor.fetchone():
                    self.logger.info(f"===> GẶP TIN CŨ: [{post_title}]. DỪNG NHÁNH QUÉT NÀY.")
                    break # Thoát vòng lặp panel hiện tại

                # 4. Trích xuất danh sách file đính kèm
                attachments = []
                for file_li in panel.css('ul.item-list li'):
                    f_name = "".join(file_li.css('a ::text').getall()).strip()
                    f_url = file_li.css('a::attr(href)').get()
                    if f_url:
                        attachments.append(f"{f_name}: {response.urljoin(f_url)}")

                # 5. Đóng gói Item
                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = post_title
                e_item['date'] = iso_date
                e_item['details_raw'] = f"{post_title}\nFiles:\n" + "\n".join(attachments)
                e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                yield e_item

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    """Xử lý định dạng DD.MM.YYYY của BVH"""
    if not vietnam_date_str:
        return None
    try:
        # BVH dùng dấu chấm nên parse trực tiếp theo định dạng này
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d.%m.%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return vietnam_date_str