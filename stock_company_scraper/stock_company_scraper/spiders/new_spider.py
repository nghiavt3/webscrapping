import scrapy
from stock_company_scraper.items import EventItem

class EventSpider(scrapy.Spider):
    name = 'event_table_scraper'
    # Thay thế bằng domain thực tế
    allowed_domains = ['finance.vietstock.vn'] 
    # Thay thế bằng URL thực tế chứa bảng dữ liệu
    start_urls = ['https://finance.vietstock.vn/CAT-ctcp-thuy-san-ca-mau.htm?back=close'] 

    def parse(self, response):
        # 1. Định vị bảng chính xác bằng ID
        # Sử dụng CSS Selector để chọn thẻ <tbody> của bảng có ID là 'event-formdata'
        table_body = response.css('table#event-formdata tbody')
        
        if not table_body:
            self.logger.warning("Không tìm thấy bảng dữ liệu với ID 'event-formdata'")
            return

        # 2. Lặp qua tất cả các hàng (<tr>) trong bảng
        # Selector 'tr' bên trong 'tbody'
        rows = table_body.css('tr')

        for row in rows:
            item = EventItem()
            
            # --- Trích xuất Dữ liệu Cột 1 (Ngày) ---
            # Chọn thẻ <td> có class 'col-date' và lấy nội dung text
            date_selector = row.css('td.col-date::text').get()
            item['date'] = date_selector.strip() if date_selector else None
            item['mcp'] = 'CAT'
            item['web_source'] = 'vietstock'
            # --- Trích xuất Dữ liệu Cột 2 (Sự kiện) ---
            # Chọn thẻ <span> có class 'event-link'
            span_selector = row.css('span.event-link')
            
            if span_selector:
                # 3. Lấy Tóm tắt (Nội dung văn bản)
                # Selector '::text' lấy nội dung bên trong thẻ <span>
                summary_selector = span_selector.css('::text').get()
                item['summary'] = summary_selector.strip() if summary_selector else None

                # 4. Lấy Chi tiết (Thuộc tính 'content')
                # Selector '::attr(content)' lấy giá trị của thuộc tính 'content'
                item['details_raw'] = span_selector.css('::attr(content)').get()
            else:
                item['summary'] = "N/A"
                item['details_raw'] = "N/A"
            
            # 5. Yield Item
            yield item