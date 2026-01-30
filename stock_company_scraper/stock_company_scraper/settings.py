# Scrapy settings for stock_company_scraper project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "stock_company_scraper"

SPIDER_MODULES = ["stock_company_scraper.spiders"]
NEWSPIDER_MODULE = "stock_company_scraper.spiders"

ADDONS = {}


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = "stock_company_scraper (+http://www.yourdomain.com)"
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36'
#USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
# Bạn có thể tìm User-Agent mới nhất bằng cách gõ "my user agent" trên Google
# Obey robots.txt rules
PLAYWRIGHT_LAUNCH_OPTIONS = {
    "headless": True,  # ĐỔI THÀNH FALSE ĐỂ QUAN SÁT
    "args": ["--disable-blink-features=AutomationControlled"], # Giấu dấu vết bot
}

PLAYWRIGHT_CONTEXT_ARGS = {
    "viewport": {"width": 1920, "height": 1080},
    "user_agent": USER_AGENT,
}
ROBOTSTXT_OBEY = False

# Concurrency and throttling settings
#CONCURRENT_REQUESTS = 16
CONCURRENT_REQUESTS_PER_DOMAIN = 1
DOWNLOAD_DELAY = 3

#PLAYWRIGHT_PROCESS_REQUEST_HEADERS = None
# Kích hoạt Playwright Downloader Middleware
DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}
ASYNCIO_EVENT_LOOP_POLICY = "asyncio.WindowsSelectorEventLoopPolicy" # Chỉ dành cho Windows
PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 60000
# Tăng timeout để Playwright có thời gian khởi động trình duyệt
DOWNLOAD_TIMEOUT = 180
# GIỚI HẠN REQUEST ĐỒNG THỜI
# Khôi phục giá trị chung an toàn (hoặc giá trị mặc định của Scrapy)
CONCURRENT_REQUESTS = 16 
CONCURRENT_REQUESTS_PER_DOMAIN = 8
# Disable cookies (enabled by default)
#COOKIES_ENABLED = False
# Tăng thời gian chờ cho quá trình dọn dẹp Playwright/AsyncIO (từ 5 giây lên 10 giây hoặc hơn)
# Điều này cho phép các tác vụ đang chờ (pending tasks) có thời gian hoàn tất.
PLAYWRIGHT_CLOSING_TIMEOUT = 10
# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False
HTTPERROR_ALLOWED_CODES=[500]
# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
#}
# DEFAULT_REQUEST_HEADERS = {
#     # Thêm Referer để giả vờ bạn đến từ một trang khác
#     'Referer': 'https://www.google.com/', 
#     # Chấp nhận các kiểu mã hóa phổ biến
#     'Accept-Encoding': 'gzip, deflate, br',
#     'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
# }
# Thêm các Header mặc định
# DEFAULT_REQUEST_HEADERS = {
#    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,webp,image/apng,*/*;q=0.8',
#    'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
#    'Referer': 'https://www.google.com/',
# }
DEFAULT_REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
    'Referer': 'https://www.google.com/',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Sec-Ch-UA': '"Not A(Brand";v="8", "Chromium";v="142", "Google Chrome";v="142"',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
}
# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    "stock_company_scraper.middlewares.StockCompanyScraperSpiderMiddleware": 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    "stock_company_scraper.middlewares.StockCompanyScraperDownloaderMiddleware": 543,
#}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
   "stock_company_scraper.pipelines.StockCompanyScraperPipeline": 300,

   # 800: Pipeline Firebase để xử lý lưu trữ/thông báo. Số càng lớn, chạy càng sau.
    #'stock_company_scraper.pipelines.FirebaseStoragePipeline': 800,
    'stock_company_scraper.pipelines.SQLiteStoragePipeline': 800,
}
# Thêm cấu hình cho Database
SQLITE_DATABASE_NAME = 'stock_events.db'
SQLITE_TABLE_NAME = 'events_history'
# settings.py
NEW_EVENTS_LOG_FILE = 'new_events_today.txt'
# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"
TWISTED_REACTOR = 'twisted.internet.asyncioreactor.AsyncioSelectorReactor'
# Set settings whose default value is deprecated to a future-proof value
FEED_EXPORT_ENCODING = "utf-8"
# 1. Kích hoạt ghi log ra file
# LOG_ENABLED = True
# LOG_FILE = 'crawling_log.txt' # Tên file log của bạn
# LOG_FILE_APPEND = True        # Ghi nối tiếp vào file cũ, không xóa dữ liệu cũ
# LOG_LEVEL = 'INFO'            # Chỉ ghi những thông tin từ mức INFO trở lên
# LOG_ENCODING = 'utf-8'        # Đảm bảo tiếng Việt không bị lỗi font
# LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
# # Quan trọng: Đảm bảo log được đẩy ra file ngay lập tức
# LOG_STDOUT = False
# # 2. Tùy chỉnh định dạng dòng log (Thời gian - Tên Spider - Nội dung)
# LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
# LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'

# Buộc Playwright đóng trình duyệt một cách nhẹ nhàng hơn
PLAYWRIGHT_ABORT_REQUEST_TIMEOUT = 1000  # ms
PLAYWRIGHT_ABORT_REQUEST = lambda req: req.resource_type in ["image", "media", "font"]
# Sử dụng Policy Event Loop của Windows (khắc phục lỗi SelectorEventLoop)
import asyncio
import sys

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())