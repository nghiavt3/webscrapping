import sqlite3
import logging
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes
import os

# Cấu hình log
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_NAME = os.path.join(BASE_DIR, 'stock_events.db')
TOKEN = "8431203903:AAE4dwx8GX_OCJiBKfiIqgwNZsF9YFK5Ewg"

def get_data_from_db(symbol, limit):
    """Truy vấn dữ liệu từ SQLite"""
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        
        # Chỉ lấy những cột cần thiết để hiển thị trên Telegram
        # Table name: event_msn, event_vnm...
        table_name = f"event_{symbol.lower()}"
        
        # Kiểm tra bảng có tồn tại không để tránh crash
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if not cursor.fetchone():
            return []

        # SQL: Lấy 3 cột chính để hiển thị
        query = f"SELECT summary, date, details_clean FROM {table_name} ORDER BY date DESC LIMIT ?"
        
        # QUAN TRỌNG: (limit,) phải có dấu phẩy để tạo thành tuple
        cursor.execute(query, (limit,))
        
        rows = cursor.fetchall()
        conn.close()
        return rows
    except Exception as e:
        logging.error(f"Lỗi database: {e}")
        return []

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Hỗ trợ cả tin nhắn cá nhân và tin nhắn trong Channel
    message = update.message if update.message else update.channel_post
    if not message or not message.text:
        return

    text = message.text.strip().lower()
    parts = text.split()
    
    if len(parts) == 2:
        symbol = parts[0]
        try:
            limit = int(parts[1])
            
            # Lấy dữ liệu (Chỉ lấy 3 cột: summary, date, details_clean)
            data = get_data_from_db(symbol, limit)
            
            if not data:
                await message.reply_text(f"❌ Không tìm thấy bảng dữ liệu `event_{symbol.upper()}` hoặc dữ liệu trống.")
                return

            response = f"📊 **{symbol.upper()} - {len(data)} tin mới nhất:**\n\n"
            
            for i, (summary, date, details_clean) in enumerate(data, 1):
                # Xử lý trường hợp date bị None như trong gui_tracker của bạn
                display_date = date if (date and date != "None") else "N/A"
                
                # Format tin nhắn
                response += f"*{i}. {summary}*\n"
                response += f"📅 Ngày: {display_date}\n"
                response += f"🔗 [Xem chi tiết tại đây]({details_clean})\n\n"
                
                # Tránh gửi tin nhắn quá dài (Telegram giới hạn 4096 ký tự)
                if len(response) > 3500:
                    #await message.reply_text(response, parse_mode='Markdown', disable_web_page_preview=True)
                    await message.reply_text(response, disable_web_page_preview=True)
                    response = ""

            if response:
                #await message.reply_text(response, parse_mode='Markdown', disable_web_page_preview=True)
                await message.reply_text(response, disable_web_page_preview=True)
            
        except ValueError:
            await message.reply_text("⚠️ Sai định dạng. Vui lòng nhắn: `msn 10`")
        except Exception as e:
            logging.error(f"Lỗi xử lý: {e}")
            await message.reply_text("🔥 Có lỗi hệ thống xảy ra.")
    else:
        # Nếu nhắn tin không đúng cấu trúc (ví dụ chỉ nhắn "hello") thì không phản hồi hoặc hướng dẫn
        pass 

if __name__ == '__main__':
    print("🚀 Bot Telegram đang lắng nghe...")
    app = Application.builder().token(TOKEN).build()
    
    # Lắng nghe tất cả tin nhắn văn bản (bao gồm cả từ Channel)
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_message))
    
    app.run_polling()