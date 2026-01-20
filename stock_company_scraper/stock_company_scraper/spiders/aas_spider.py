import scrapy
import sqlite3
import os
from stock_company_scraper.items import EventItem
from datetime import datetime
import re

class EventSpider(scrapy.Spider):
    name = 'event_aas'
    mcpcty = 'AAS'
    allowed_domains = ['aas.com.vn'] 
    start_urls = ['https://aas.com.vn/danh-muc-thong-tin-co-dong/cong-bo-thong-tin/'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        # Đường dẫn file db
        self.db_path = 'stock_events.db'

    def start_requests(self):
        yield scrapy.Request(
            url=self.start_urls[0],
            callback=self.parse,
            # Giữ nguyên Playwright để render trang web động
            meta={'playwright': True}
        )
    
    def parse(self, response):
        # Mở kết nối SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Đảm bảo bảng tồn tại (Sử dụng cấu trúc chuẩn bạn đã sửa)
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

        # Lặp qua từng item tin tức
        items = response.css('.research-item.news')
        
        for item in items:
            # Trích xuất dữ liệu cơ bản
            title = item.css('.content a.text-body-lg-semibold::text').get()
            detail_url = item.css('.content a.text-body-lg-semibold::attr(href)').get()
            publish_date = item.css('.content .flex.items-center.gap-2 p::text').get()
            summary = item.css('.content p.text-body-sm-regular.text-text-tertiary::text').get()
            
            # Làm sạch dữ liệu
            title = title.strip() if title else ""
            iso_date = convert_date_to_iso8601(publish_date.strip() if publish_date else "")

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{title}_{iso_date}".replace('/', '-').replace('.', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{title}]. DỪNG QUÉT GIA TĂNG.")
                conn.close()
                break # DỪNG TOÀN BỘ SPIDER

            # Trích xuất danh sách tài liệu đính kèm (nếu cần đưa vào details_raw)
            attachments_str = ""
            for doc in item.css('a.link-green'):
                doc_name = doc.css('::text').get()
                doc_link = doc.css('::attr(href)').get()
                if doc_link:
                    attachments_str += f"\n📎 File: {doc_name.strip()} - {response.urljoin(doc_link)}"

            # Đưa dữ liệu vào Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = title
            e_item['details_raw'] = f"{title}\n{summary}\nLink: {detail_url}{attachments_str}"
            e_item['date'] = iso_date 
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            yield e_item

        conn.close()

# Giữ nguyên hàm convert của bạn
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