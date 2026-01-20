# webscrapping
chương trình theo dõi website khác và thông báo về sự thay đổi trong cấu trúc web
# chạy chương trình Class event_table_scraper trong new_spider.py và xuất kết quả ra file news,json 
scrapy crawl event_eib -o news.json
# chạy chương trình GUI
python gui_tracker.py
# file auto_run.py Cứ mỗi 10 phút nó tự quét, nếu thấy tin mới là nó tự "bắn" tin nhắn vào điện thoại của bạn ngay lập tức