import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
from scrapy_playwright.page import PageMethod
import json
class EventSpider(scrapy.Spider):
    name = 'event_hut'
    mcpcty = 'HUT'
    allowed_domains = ['tasco.com.vn'] 

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'

    async def start(self):
        urls = [
            ('https://www.tasco.com.vn/ir#thong-tin-cong-bo', self.parse_generic),
            ('https://www.tasco.com.vn/ir#dai-hoi-co-dong', self.parse_generic),    
        ]
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                callback=callback,
                meta={
                "playwright": True,
                "playwright_page_methods": [
                    # Chờ cho đến khi danh sách <li> chứa dữ liệu xuất hiện
                    PageMethod("wait_for_selector", "li.p-25.border-bottom"),
                ],
            },
            )

        payload = {
            "query": """
            {
              posts(
                where: {categoryName: "bao-cao-tai-chinh", metaQuery: {metaArray: {key: "nam", value: "2025"}}},
                first: 100
              ) {
                nodes {
                  id
                  title
                  slug
                  content
                  date
                  featuredImage {
                    node {
                      slug
                      mediaItemUrl
                    }
                  }
                  baoCaoTaiChinhAcf {
                    fieldGroupName
                    kieuBaoCao
                    kyBaoCao
                    nam
                  }
                }
              }
            }
            """
        }
        yield scrapy.Request(
            url='https://dash.tasco.com.vn/graphql',
            method='POST',
            body=json.dumps(payload),
            headers={'Content-Type': 'application/json'},
            callback=self.parse
        )
    async def parse(self, response):
        # 1. Khởi tạo SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')
        # Chuyển đổi phản hồi thành dictionary
        raw_data = json.loads(response.text)
        nodes = raw_data.get('data', {}).get('posts', {}).get('nodes', [])

        for node in nodes:
            # 1. Trích xuất thông tin từ ACF (năm, kỳ, kiểu báo cáo)
            acf = node.get('baoCaoTaiChinhAcf', {})
            
            # 2. Xử lý phần 'content' (chứa HTML thô)
            # Chúng ta dùng scrapy.Selector để parse đoạn HTML này
            content_html = node.get('content', '')
            content_selector = scrapy.Selector(text=content_html)
            
            # Lấy link từ thuộc tính data của object hoặc href của thẻ a
            pdf_url = content_selector.css('a::attr(href)').get()


            yield {
                'id': node.get('id'),
                'nam': acf.get('nam'),
                'ky_bao_cao': acf.get('kyBaoCao'),
                'kieu_bao_cao': acf.get('kieuBaoCao'),
                'date_posted': node.get('date'),
                'file_url': pdf_url,
                'slug': node.get('slug')
            }
            summary = f"{acf.get('nam')}-{acf.get('kyBaoCao')}-{acf.get('kieuBaoCao')}"
            iso_date = datetime.fromisoformat(node.get('date')).strftime('%Y-%m-%d')
            absolute_url = (pdf_url)

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
    async def parse_generic(self, response):
        """Hàm parse dùng chung cho các chuyên mục của SeABank"""
        # 1. Khởi tạo SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')

        # 2. Duyệt qua các thẻ section dành cho giao diện Desktop (chứa đầy đủ data)
        items = response.css('li.p-25.border-bottom')

        for item in items:
            title = item.css('h5.h5::text').get()
            relative_url = item.css('a::attr(href)').get()
            date_str = item.css('span.xsmall::text').get()

            if not title or not date_str:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso8601(date_str)
            absolute_url = response.urljoin(relative_url)

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