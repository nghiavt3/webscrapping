import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime

class EventSpider(scrapy.Spider):
    name = 'event_dxg'
    mcpcty = 'DXG'
    allowed_domains = ['ir.datxanh.vn'] 
    start_urls = ['https://ir.datxanh.vn/cong-bo-thong-tin'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    def parse(self, response):
        # 1. Kết nối SQLite và chuẩn bị bảng
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')


        # 2. Nhắm mục tiêu vào tab "Công bố thông tin bất thường"
        container = response.css('#pills-unusual .filter-section-content')

        # Duyệt qua các khối năm (Đất Xanh thường hiển thị năm gần nhất lên đầu)
        for year_block in container.css('.year-element'):
            # Duyệt qua từng mục văn bản
            for item in year_block.css('.vanban-cbttbt.search-element'):
                title = item.css('a.search-query::text').get()
                detail_url = item.css('a.search-query::attr(href)').get()
                
                # Khối thông tin bổ sung chứa Ngày và Nút Tải
                date_download_block = item.css('.accordion-body')
                date_published = date_download_block.css('span::text').get()
                download_url = date_download_block.css('a[download]::attr(href)').get()
                
                if not title:
                    continue

                cleaned_title = title.strip()
                iso_date = convert_date_to_iso8601(date_published.strip())
                
                # Ưu tiên lấy download_url (PDF trực tiếp) nếu có
                final_link = response.urljoin(download_url if download_url else detail_url)

                # -------------------------------------------------------
                # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
                # -------------------------------------------------------
                event_id = f"{cleaned_title}_{iso_date}".replace(' ', '_').strip()[:150]
                
                cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
                if cursor.fetchone():
                    # Đất Xanh sắp xếp theo năm, nên nếu gặp tin cũ trong khối năm hiện tại thì có thể dừng
                    self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                    break 

                # 4. Yield Item
                e_item = EventItem()
                e_item['mcp'] = self.mcpcty
                e_item['web_source'] = self.allowed_domains[0]
                e_item['summary'] = cleaned_title
                e_item['date'] = iso_date
                e_item['details_raw'] = f"{cleaned_title}\nLink tài liệu: {final_link}"
                e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                yield e_item

        # 2. Nhắm mục tiêu vào tab "Kết quả kinh doanh"
        rows = response.css('#pills-kqkd div.table-data-filter-kqkd table tbody tr')
        # Lấy danh sách tiêu đề quý từ header (Quý 1, Quý 2...)
        quarters = response.css('#pills-kqkd div.table-data-filter-kqkd table thead th.fw-semibold::text').getall()
        for row in rows:
            # Kiểm tra nếu hàng này là hàng tiêu đề phụ (có class bg-primary) thì bỏ qua hoặc xử lý riêng
            if row.css('td.bg-primary'):
                section_name = row.css('td::text').get().strip()
                self.logger.info(f"Đang xử lý mục: {section_name}")
                continue

            # Trích xuất tên loại báo cáo
            report_name = row.css('td[scope="row"]::text').get()
            if not report_name:
                continue
            
            report_name = report_name.strip()

            # Lấy tất cả các cột dữ liệu (4 cột tương ứng 4 quý)
            cells = row.css('td')[1:] # Bỏ qua cột đầu tiên chứa tên báo cáo
            data = {
                'loai_bao_cao': report_name,
                'chi_tiet': []
            }
            for index, cell in enumerate(cells):
                # Mỗi cell có thể có link và ngày tháng
                pdf_link = cell.css('a::attr(href)').get()
                publish_date = cell.css('span::text').get()

                # Chỉ lấy dữ liệu nếu có link thực tế (tránh các link '#' trống)
                if pdf_link and pdf_link != "#":
                    cleaned_title = f"Quý {quarters[index] if index < len(quarters) else f"Q{index+1}"}-{report_name}"
                    iso_date = convert_date_to_iso8601(publish_date.strip())
                    final_link = response.urljoin(pdf_link)

                    # -------------------------------------------------------
                    # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
                    # -------------------------------------------------------
                    event_id = f"{cleaned_title}_{iso_date}".replace(' ', '_').strip()[:150]
                    
                    cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
                    if cursor.fetchone():
                        # Đất Xanh sắp xếp theo năm, nên nếu gặp tin cũ trong khối năm hiện tại thì có thể dừng
                        self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                        break 

                    # 4. Yield Item
                    e_item = EventItem()
                    e_item['mcp'] = self.mcpcty
                    e_item['web_source'] = self.allowed_domains[0]
                    e_item['summary'] = cleaned_title
                    e_item['date'] = iso_date
                    e_item['details_raw'] = f"{cleaned_title}\nLink tài liệu: {final_link}"
                    e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    yield e_item


        # 2. Nhắm mục tiêu vào tab "Công bố thông tin bất thường"
        # báo cáo thường niên
        first_table_rows = response.css('div#pills-periodic table:first-of-type tbody tr')
        for row in first_table_rows:
            title = row.css('th a::text').get().strip()            
            date_published = row.css('td:nth-child(3)::text').get().strip()
            download_url = row.css('td:nth-child(4) a::attr(href)').get()
            
            if not title:
                continue

            cleaned_title = title.strip()
            iso_date = convert_date_to_iso8601(date_published.strip())
            
            # Ưu tiên lấy download_url (PDF trực tiếp) nếu có
            final_link = response.urljoin(download_url if download_url else detail_url)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{cleaned_title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                # Đất Xanh sắp xếp theo năm, nên nếu gặp tin cũ trong khối năm hiện tại thì có thể dừng
                self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{cleaned_title}\nLink tài liệu: {final_link}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
            yield e_item

        #đại hội cổ đông
        second_table_rows = response.css('div#pills-periodic div[name="wrap-filter-table-dhcd"] tbody tr')
        for row in second_table_rows:
            title = row.css('th a::text').get()           
            date_published = row.css('td.text-muted::text').get()
            download_url = row.css('td a::attr(href)').get()
            
            if not title:
                continue

            cleaned_title = title.strip()
            iso_date = convert_date_to_iso8601(date_published.strip())
            
            # Ưu tiên lấy download_url (PDF trực tiếp) nếu có
            final_link = response.urljoin(download_url)

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            event_id = f"{cleaned_title}_{iso_date}".replace(' ', '_').strip()[:150]
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                # Đất Xanh sắp xếp theo năm, nên nếu gặp tin cũ trong khối năm hiện tại thì có thể dừng
                self.logger.info(f"===> GẶP TIN CŨ: [{cleaned_title}]. DỪNG QUÉT.")
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = cleaned_title
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{cleaned_title}\nLink tài liệu: {final_link}"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
            yield e_item    
        conn.close()

def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    input_format = "%d/%m/%Y"    
    output_format = '%Y-%m-%d'
    try:
        return datetime.strptime(vietnam_date_str.strip(), input_format).strftime(output_format)
    except Exception:
        return None