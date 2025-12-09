# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from firebase_admin import credentials, initialize_app,get_app, App, firestore
import logging
import os


class StockCompanyScraperPipeline:
    def process_item(self, item, spider):
        if item.get('details_raw'):
            details = item['details_raw']

            # 1. Thay thế mã hóa HTML thành ký tự dễ đọc
            details_clean = details.replace('&lt;br&gt;', '\n')
            details_clean = details_clean.replace('&amp;nbsp;', ' ')
            details_clean = details_clean.replace('<br>', '\n')
            details_clean = details_clean.replace('&nbsp;',' ')
            details_clean = details_clean.replace('v\xa0 \xa0 \xa0 \xa0 \xa0', ' ') # Xử lý các khoảng trắng unicode
            
            
            # Thêm trường sạch vào Item (hoặc thay thế trường raw)
            item['details_clean'] = details_clean
            
            # Xóa trường thô (tùy chọn)
            del item['details_raw']
        return item


# stock_company_scraper/pipelines.py

class FirebaseStoragePipeline:
    
    def __init__(self):
        # 1. Khởi tạo Firebase
        # Đường dẫn tới file key đã tải xuống
        key_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'firebase-admin-key.json')
        print("duong dan ne :" + key_path)
        logging.info("Kết nối " + key_path)
        try:
            cred = credentials.Certificate(key_path)
            # Thay thế bằng tên dự án Firebase của bạn nếu cần
            #if not len(list(App._get_service())):
            initialize_app(cred, {'projectId': 'yourwebscrapping-d300c'}) 
            self.db = firestore.client()
            logging.info("Kết nối Firebase Firestore thành công.")
        except Exception as e:
            logging.error(f"Lỗi kết nối Firebase: {e}")
            self.db = None
            
        # Tên collection trong Firestore để lưu dữ liệu
        self.collection_name = 'stock_events'

    def process_item(self, item, spider):
        if not self.db:
            logging.warning("Bỏ qua Item vì không kết nối được Firebase.")
            return item

        # Chuyển Item Scrapy sang dictionary
        data = dict(item)
        
        # Tạo ID duy nhất cho sự kiện để so sánh. 
        # Kết hợp Tóm tắt và Ngày là một cách hiệu quả để xác định tính duy nhất
        event_id = f"{data.get('summary')}_{data.get('date')}"
        
        # 2. LÀM SẠCH ID: Thay thế ký tự '/' bằng '-' hoặc xóa nó.
         # Tên Document ID không được chứa ký tự '/'
        event_id_clean = event_id.replace('/', '-') 
        event_id_clean = event_id_clean.replace('.', '_') # Thay thế dấu chấm bằng gạch dưới

        # 3. Loại bỏ các khoảng trắng thừa ở đầu/cuối
        event_id_clean = event_id_clean.strip()

        # 4. Tránh ID quá dài: Giới hạn độ dài ID (tùy chọn)
        if len(event_id_clean) > 150:
            event_id_clean = event_id_clean[:150]



        # 2. So sánh Dữ liệu Cũ và Mới (Kiểm tra xem tài liệu đã tồn tại chưa)
        doc_ref = self.db.collection(self.collection_name).document(event_id_clean)
        
        if doc_ref.get().exists:
            # Dữ liệu đã có trong Database
            logging.info(f"Sự kiện đã tồn tại, không cần thông báo: {event_id_clean}")
        else:
            # Dữ liệu MỚI!
            logging.info(f"SỰ KIỆN MỚI ĐƯỢC PHÁT HIỆN: {event_id_clean}")
            
            # 3. Lưu dữ liệu mới vào Firestore
            doc_ref.set(data)
            logging.info(f"Đã lưu dữ liệu mới vào Firebase: {event_id_clean}")
            
            # 4. Phát thông báo (Chức năng Thông báo)
            self._send_notification(data)
            
        return item

    def _send_notification(self, data):
        """
        Thực hiện logic gửi thông báo (Telegram, Email, Desktop, v.v.)
        """


        try:
            summary = data.get('summary', 'Sự kiện mới')
            date = data.get('date', 'N/A')
            # **********************************************
            # VÍ DỤ CHỨC NĂNG THÔNG BÁO THỰC TẾ
            # **********************************************
            
            # Ví dụ đơn giản: In ra console để kiểm tra
            print("\n==============================================")
            print(f"🚨 THÔNG BÁO: SỰ KIỆN CÔNG TY MỚI! 🚨")
            print(f"Ngày: {date}")
            print(f"Tóm tắt: {summary}")
            print("==============================================")
            
            # **********************************************
            # Nếu dùng Email/Telegram, bạn sẽ đặt mã gọi API ở đây
            # Ví dụ: send_telegram_message(f"Tin mới CAT: {summary}")
            # **********************************************
            
        except Exception as e:
            logging.error(f"Lỗi khi gửi thông báo: {e}")


import sqlite3
from plyer import notification # Thêm import plyer
#import logging
import json # Import thư viện json
# Lấy tên database và bảng từ settings (hoặc đặt mặc định nếu không có)
DATABASE_NAME = 'stock_events.db'
#TABLE_NAME = 'events_history'
class SQLiteStoragePipeline:
    
    def open_spider(self, spider):
        """Kết nối database và tạo bảng nếu chưa tồn tại."""
        # Lấy cấu hình từ settings.py
        self.db_name = spider.settings.get('SQLITE_DATABASE_NAME', DATABASE_NAME)
       # self.table_name = spider.settings.get('SQLITE_TABLE_NAME', TABLE_NAME)
        self.table_name = spider.name
        # Kết nối
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        
        # Tạo bảng (Nếu 'summary' và 'date' là trường dữ liệu chính để so sánh)
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id INTEGER PRIMARY KEY,
                unique_key TEXT UNIQUE,  -- Trường dùng để so sánh (đảm bảo tính duy nhất)
                mcp TEXT,
                summary TEXT,
                date TEXT,
                web_source TEXT,
                details_clean TEXT,
                scraped_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.commit()
        logging.info(f"Đã mở kết nối SQLite và tạo bảng '{self.table_name}'.")
        # Lấy tên file log từ settings
        self.log_file = spider.settings.get('NEW_EVENTS_LOG_FILE', 'new_events_today.txt')
        
        # Dọn dẹp file log cũ (để mỗi lần chạy là dữ liệu mới trong ngày)
        if os.path.exists(self.log_file):
            os.remove(self.log_file)

    def close_spider(self, spider):
        """Đóng kết nối database khi Spider kết thúc."""
        self.conn.close()
        logging.info("Đã đóng kết nối SQLite.")
        
    def process_item(self, item, spider):
        """Xử lý, so sánh và lưu trữ Item."""
        data = dict(item)
        
        # 1. Tạo Khóa Duy nhất (UNIQUE KEY)
        # Sử dụng Tóm tắt và Ngày để tạo khóa so sánh
        unique_key = f"{data.get('summary', '')}_{data.get('date', '')}".replace('/', '-').strip()
        
        # 2. So sánh: Kiểm tra xem unique_key đã tồn tại chưa
        self.cursor.execute(f"SELECT id FROM {self.table_name} WHERE unique_key = ?", (unique_key,))
        
        if self.cursor.fetchone():
            # Dữ liệu ĐÃ TỒN TẠI
            spider.logger.info(f"Sự kiện đã tồn tại, bỏ qua: {unique_key[:50]}...")
            return item
        else:
            # Dữ liệu MỚI!
            spider.logger.info(f"🚨 SỰ KIỆN MỚI ĐƯỢC PHÁT HIỆN: {unique_key[:50]}...")
            
            # 3. Lưu dữ liệu mới vào SQLite
            try:
                self.cursor.execute(f"""
                    INSERT INTO {self.table_name} (unique_key,mcp, summary, date,web_source, details_clean)
                    VALUES (?, ?, ?, ?, ?,?)
                """, (
                    unique_key,
                    data.get('mcp'),
                    data.get('summary'),
                    data.get('date'),
                    data.get('web_source'),
                    data.get('details_clean', ''), # Giả định bạn có trường full_content
                    
                ))
                self.conn.commit()
                # Ghi dữ liệu mới vào file log
                with open(self.log_file, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(data, ensure_ascii=False) + '\n')
                # 4. Phát thông báo
                if not os.path.exists('__notified_today'):
                    self._send_notification(data)
                    # Tạo cờ để tránh thông báo liên tục nếu có nhiều tin mới
                    open('__notified_today', 'w').close()
                
            except sqlite3.Error as e:
                spider.logger.error(f"Lỗi khi INSERT vào SQLite: {e}")

            return item

    def _send_notification(self, data):

        # Lấy đường dẫn tuyệt đối của file log
        log_file_path = os.path.abspath(self.log_file)
        """Thực hiện chức năng gửi thông báo (in ra console để ví dụ)."""
        summary = data.get('summary', 'Sự kiện mới không rõ ràng')
        date = data.get('date', 'N/A')
        # Tiêu đề thông báo
        title = "📢 SỰ KIỆN CỔ PHIẾU MỚI"
        
        # Nội dung thông báo
        message = f"Mã: CAT | Ngày: {date}\n{summary}"
        try:

            # Vì plyer không hỗ trợ click, chúng ta sẽ in lệnh ra màn hình để
            # người dùng tự chạy hoặc tích hợp vào hệ thống lập lịch (Cron/Task Scheduler)
        
            if os.name == 'nt': # Windows
                open_command = f'start "" "{log_file_path}"'
            elif os.name == 'posix': # Linux/macOS
                open_command = f'open "{log_file_path}"' # Hoặc 'xdg-open' trên Linux
            else:
                open_command = f"Vui lòng mở file: {log_file_path}"
            # Gửi thông báo Desktop
            notification.notify(
                title=title,
                message=message,
                # Tên ứng dụng hiển thị trong thông báo
                app_name='Scrapy Stock Tracker', 
                # Icon sẽ hiển thị (chỉ hoạt động với file .ico trên Windows)
                # app_icon='path/to/icon.ico', 
                timeout=10 # Thời gian hiển thị (giây)
            )
           # self.spider.logger.info(f"LỆNH MỞ FILE LOG: {open_command}")
            
        except Exception as e:
            # Thông báo nếu plyer không thể gửi (ví dụ: thiếu dependency của OS)
            self.spider.logger.error(f"❌ Lỗi gửi thông báo Desktop (Plyer): {e}")
        print("\n==============================================")
        print("🔔 THÔNG BÁO: SỰ KIỆN CỔ PHIẾU MỚI! 🔔")
        print(f"Ngày: {date}")
        print(f"Tóm tắt: {summary}")
        print("==============================================")
        
        # NOTE: Bạn có thể tích hợp API Telegram, Zalo, Email ở đây.