import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_dxs'
    mcpcty = 'DXS'
    allowed_domains = ['datxanhservices.vn'] 
    start_urls = ['https://datxanhservices.vn/quan-he-co-dong/cong-bo-thong-tin/'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
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

        # 2. Lặp qua từng nhóm Năm (tab-list table-ttbt)
        year_groups = response.css('div.tab-list.table-ttbt')
        
        for group in year_groups:
            # Lặp qua từng mục tài liệu trong nhóm Năm này
            document_items = group.css('div.document-items div.item')

            for item in document_items:
                title = item.css('a.item-title::text').get()
                doc_url = item.css('a.item-title::attr(href)').get()
                date_raw = item.css('.item-date::text').get()
                download_url = item.css('a[download]::attr(href)').get()
                
                if not title:
                    continue

                cleaned_title = title.strip()
                iso_date = convert_date_to_iso8601(date_raw)
                
                # Ưu tiên link download trực tiếp file PDF
                final_link = response.urljoin(download_url if download_url else doc_url)

                # -------------------------------------------------------
                # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
                # -------------------------------------------------------
                event_id = f"{cleaned_title}_{iso_date}".replace(' ', '_').strip()[:150]
                
                cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
                if cursor.fetchone():
                    self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                    # Vì DXS sắp xếp tin mới nhất lên đầu, ta có thể dừng ngay
                    break 

                # 4. Yield Item
                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = cleaned_title
                e_item['date'] = iso_date
                e_item['details_raw'] = f"{cleaned_title}\nTài liệu: {final_link}"
                e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                yield e_item

        year_panes = response.css('div#pills-cate-268 .tab-content .tab-pane')

        for pane in year_panes:
            # Lấy năm từ ID (ví dụ 'year-2025' -> '2025')
            year = pane.attrib.get('id', '').replace('year-', '')
            
            # Duyệt từng hàng trong bảng của năm đó
            rows = pane.css('table tbody tr')
            
            for row in rows:
                # Bỏ qua các hàng tiêu đề nhóm (có class 'heading')
                if row.css('.heading'):
                    continue
                    
                report_name = row.css('td:first-child::text').get()
                if report_name:
                    report_name = report_name.strip()

                # Lấy dữ liệu của 4 quý (tương ứng 4 cột td tiếp theo)
                quarters = row.css('td')[1:] # Bỏ cột đầu tiên là tên báo cáo
                
                for i, q_data in enumerate(quarters):
                    link = q_data.css('a::attr(href)').get()
                    date = q_data.css('span::text').get()
                    
                    # Chỉ yield nếu có dữ liệu (link hoặc ngày)
                    if link or date:
                        cleaned_title = f"{year}-quý{i+1}-{report_name}"
                        iso_date = convert_date_to_iso8601(date)
                        
                        # Ưu tiên link download trực tiếp file PDF
                        final_link = response.urljoin(link)

                        # -------------------------------------------------------
                        # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
                        # -------------------------------------------------------
                        event_id = f"{cleaned_title}_{iso_date}".replace(' ', '_').strip()[:150]
                        
                        cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
                        if cursor.fetchone():
                            self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                            # Vì DXS sắp xếp tin mới nhất lên đầu, ta có thể dừng ngay
                            break 

                        # 4. Yield Item
                        e_item = EventItem()
                        e_item['mcp'] = self.mcpcty
                        e_item['web_source'] = self.allowed_domains[0]
                        e_item['summary'] = cleaned_title
                        e_item['date'] = iso_date
                        e_item['details_raw'] = f"{cleaned_title}\nTài liệu: {final_link}"
                        e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        
                        yield e_item    
        
        year_tabs = response.css('#pills-cate-280 .tab-content > .tab-pane')

        for tab in year_tabs:
            # Lấy ID và tách lấy năm (ví dụ: "year-280-2025" -> "2025")
            tab_id = tab.attrib.get('id', '')
            year = tab_id.split('-')[-1] if tab_id else "N/A"

            # 2. Duyệt qua từng dòng trong bảng của năm đó
            rows = tab.css('table tbody tr.item')
            
            for row in rows:
                # Trích xuất dữ liệu chi tiết
                report_name = row.css('td:nth-child(1) a.item-title::text').get()
                report_type = row.css('td:nth-child(2) strong::text').get()
                updated_date = row.css('td:nth-child(3)::text').get()
                download_link = row.css('td:nth-child(4) a::attr(href)').get()
                cleaned_title = f"{report_type}-{report_name}"
                iso_date = convert_date_to_iso8601(updated_date)
                
                # Ưu tiên link download trực tiếp file PDF
                final_link = response.urljoin(download_link)

                # -------------------------------------------------------
                # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
                # -------------------------------------------------------
                event_id = f"{cleaned_title}_{iso_date}".replace(' ', '_').strip()[:150]
                
                cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
                if cursor.fetchone():
                    self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                    # Vì DXS sắp xếp tin mới nhất lên đầu, ta có thể dừng ngay
                    break 

                # 4. Yield Item
                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = cleaned_title
                e_item['date'] = iso_date
                e_item['details_raw'] = f"{cleaned_title}\nTài liệu: {final_link}"
                e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                yield e_item    

        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    input_format = '%d/%m/%Y'
    output_format = '%Y-%m-%d'
    try:
        # Làm sạch chuỗi ngày trước khi parse
        date_obj = datetime.strptime(vietnam_date_str.strip(), input_format)
        return date_obj.strftime(output_format)
    except ValueError:
        return None