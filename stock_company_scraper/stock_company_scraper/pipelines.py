import sqlite3
import requests
import logging
import html  # Thêm thư viện này để xử lý ký tự HTML
from itemadapter import ItemAdapter

class StockCompanyScraperPipeline:
    """Pipeline làm sạch dữ liệu thô."""
    def process_item(self, item, spider):
        if item.get('details_raw'):
            details = str(item['details_raw'])
            # Làm sạch HTML cơ bản
            details_clean = details.replace('&lt;br&gt;', '\n').replace('&amp;nbsp;', ' ')
            details_clean = details_clean.replace('<br>', '\n').replace('&nbsp;', ' ')
            # Loại bỏ các khoảng trắng đặc biệt của unicode
            details_clean = details_clean.replace('\xa0', ' ')
            
            item['details_clean'] = details_clean.strip()
            # Xóa raw để tiết kiệm bộ nhớ
            if 'details_raw' in item:
                del item['details_raw']
        return item

class SQLiteStoragePipeline:
    def __init__(self):
        # 1. Cấu hình Telegram (Sử dụng Token và ID của bạn)
        self.tele_token = "8586036700:AAFWRMSt985_aoI8U5LheWIatJSymCW8biI"
        self.tele_chat_id = "-1003249872525"
        
        # 2. Cấu hình Database
        self.db_name = 'stock_events.db'

    def process_item(self, item, spider):
        table_name = f"{spider.name}"
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

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

        # Tạo ID duy nhất (Sử dụng summary và date)
        summary_for_id = item.get('summary', 'no_title')
        date_for_id = item.get('date') or 'NODATE'
        event_id = f"{summary_for_id}_{date_for_id}"
        event_id_clean = event_id.replace('/', '-').replace('.', '_').replace(' ', '_').strip()[:150]

        # KIỂM TRA TIN ĐÃ TỒN TẠI CHƯA
        cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id_clean,))
        if cursor.fetchone():
            logging.info(f"--- Tin đã tồn tại: {item.get('mcp')} ---")
        else:
            try:
                cursor.execute(f'''
                    INSERT INTO {table_name} (id, mcp, date, summary, scraped_at, web_source, details_clean)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    event_id_clean,
                    item.get('mcp'),
                    item.get('date'),
                    item.get('summary'),
                    item.get('scraped_at'),
                    item.get('web_source'),
                    item.get('details_clean')
                ))
                conn.commit()
                logging.info(f"🆕 ĐÃ LƯU TIN MỚI VÀ GỬI TELEGRAM: {item.get('mcp')}")

                # Gửi thông báo Telegram cho tin mới
                #self._send_telegram_notification(item)
            except Exception as e:
                logging.error(f"Lỗi lưu SQLite: {e}")

        conn.close()
        return item

    def _send_telegram_notification(self, data):
        """Hàm gửi tin nhắn HTML tới Telegram với xử lý lỗi ký tự đặc biệt."""
        mcp = str(data.get('mcp', 'N/A')).upper()
        # Quan trọng: html.escape giúp tránh lỗi 400 khi summary có ký tự <, >, &
        summary = html.escape(str(data.get('summary', 'Không có tiêu đề')))
        date = html.escape(str(data.get('date', 'N/A')))
        source = html.escape(str(data.get('web_source', 'Nguồn tin')))
        
        # Lấy link từ details_clean nếu có (thường link ở dòng cuối)
        details = data.get('details_clean', '')
        
        message = (
            f"🔔 <b>PHÁT HIỆN TIN MỚI: {mcp}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📝 <b>Nội dung:</b> {summary}\n"
            f"📝 <b>Link:</b> {details}\n"
            f"📅 <b>Ngày:</b> {date}\n"
            f"🌐 <b>Nguồn:</b> {source}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🚀 <i>Hệ thống quét tự động của bạn</i>"
        )

        url = f"https://api.telegram.org/bot{self.tele_token}/sendMessage"
        payload = {
            "chat_id": self.tele_chat_id,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": False
        }

        try:
            response = requests.post(url, data=payload, timeout=15)
            if response.status_code != 200:
                logging.error(f"Telegram API Error {response.status_code}: {response.text}")
        except Exception as e:
            logging.error(f"Không thể kết nối Telegram: {e}")