import sqlite3
import logging
import os
import re
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, MessageHandler, filters, ContextTypes, CallbackQueryHandler
# Thêm thư viện cần thiết ở đầu file
import requests
import google.generativeai as genai
# --- 1. CẤU HÌNH ---
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_NAME = os.path.join(BASE_DIR, 'stock_events.db')
TOKEN = "8431203903:AAE4dwx8GX_OCJiBKfiIqgwNZsF9YFK5Ewg"
# --- CẤU HÌNH AI (Giống bên GUI) ---
try:
    genai.configure(api_key="AIzaSyCaGKXaOKGFRq73Qh-psbglhTkCkxpkpPw")
    AI_MODEL = genai.GenerativeModel('gemini-flash-latest')
except Exception as e:
    logging.error(f"Lỗi khởi tạo AI: {e}")
# --- HÀM HỖ TRỢ PHÂN TÍCH (Logic từ gui_tracker) ---

def clean_pdf_url(raw_url):
    from urllib.parse import unquote
    # Giải mã URL để xử lý các ký tự đặc biệt
    raw_url = unquote(raw_url.strip())
    
    # Xử lý link Google Drive
    if 'drive.google.com' in raw_url:
        # Regex này bắt được ID từ cả link /file/d/.../view và link /uc?id=...
        file_id_match = re.search(r'(?:/d/|id=)([a-zA-Z0-9_-]+)', raw_url)
        if file_id_match:
            file_id = file_id_match.group(1)
            return f"https://drive.google.com/uc?export=download&id={file_id}"
    
    return raw_url

async def analyze_pdf_via_ai(pdf_url):
    try:
        pdf_url = clean_pdf_url(pdf_url)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Sử dụng session để giữ cookie xác nhận của Google
        session = requests.Session()
        response = session.get(pdf_url, headers=headers, timeout=45, verify=False)
        
        # Xử lý xác nhận mã độc của Google Drive nếu file nặng
        if 'confirm=' in response.text and 'drive.google.com' in pdf_url:
            confirm_match = re.search(r'confirm=([a-zA-Z0-9_-]+)', response.text)
            if confirm_match:
                confirm_token = confirm_match.group(1)
                # Gọi lại link với token xác nhận
                response = session.get(pdf_url + "&confirm=" + confirm_token, headers=headers, timeout=45, verify=False)

        pdf_blob = response.content
        
        # Kiểm tra header PDF (đúng như logic bên GUI)
        if b"%PDF" not in pdf_blob[:1024]:
            logging.error(f"Nội dung nhận được không phải PDF: {pdf_blob[:100]}")
            return "❌ Lỗi: Link Google Drive này yêu cầu quyền truy cập (chế độ Công khai) hoặc không cho phép tải trực tiếp."

        prompt = """
        Hãy phân tích file PDF đính kèm:
        1. Tóm tắt 3 nội dung quan trọng nhất ảnh hưởng đến doanh nghiệp.
        2. Đánh giá tác động đến giá cổ phiếu: Tích cực, Tiêu cực hay Trung tính?
        3. Chấm điểm mức độ ảnh hưởng: Từ -10 (Rất xấu) đến +10 (Rất tốt).
        4. So sánh với dữ liệu cùng kỳ và định giá theo p/b, p/e.
        Yêu cầu trả lời bằng tiếng Việt, ngắn gọn và trực diện.
        """
        
        response_ai = AI_MODEL.generate_content([
            prompt,
            {"mime_type": "application/pdf", "data": pdf_blob}
        ])
        return response_ai.text
        
    except Exception as e:
        return f"❌ Lỗi hệ thống: {str(e)}"

def save_to_ai_cache(pdf_url,symbol, result_text):
    try:
        pdf_url = clean_pdf_url(pdf_url)
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        # Vì link từ bot thường không có mã CP đi kèm ngay lập tức, ta để mcp là 'TELEGRAM_LINK'
        cursor.execute("INSERT OR REPLACE INTO ai_cache (pdf_url, mcp, analysis_result) VALUES (?, ?, ?)", 
                       (pdf_url, symbol, result_text))
        conn.commit()
        conn.close()
        logging.info(f"Đã lưu cache AI cho mã {symbol}")
    except Exception as e:
        logging.error(f"Lỗi lưu cache AI: {e}")  

def get_cached_ai(pdf_url):
    """Lấy kết quả từ bảng cache nếu link đã tồn tại"""
    try:
        # Quan trọng: Làm sạch link trước khi tìm kiếm để khớp với link đã lưu
        clean_url = clean_pdf_url(pdf_url)
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        cursor.execute("SELECT analysis_result FROM ai_cache WHERE pdf_url = ?", (clean_url,))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else None
    except Exception as e:
        logging.error(f"Lỗi truy vấn cache: {e}")
        return None
# --- 2. HÀM TRUY VẤN DATABASE ---

def get_data_from_db(symbol, limit):
    """Lấy danh sách tin tức từ bảng event_xxx"""
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        table_name = f"event_{symbol.lower()}"
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if not cursor.fetchone(): return []

        # Lấy thêm cột ID (nếu có) hoặc dùng rowid để định danh chính xác bản tin
        query = f"SELECT summary, date, details_clean, rowid FROM {table_name} ORDER BY date DESC LIMIT ?"
        cursor.execute(query, (limit,))
        rows = cursor.fetchall()
        conn.close()
        return rows
    except Exception as e:
        logging.error(f"Lỗi DB News: {e}")
        return []

def get_detail_by_rowid(symbol, rowid):
    """Lấy nội dung chi tiết của một bản tin cụ thể qua rowid"""
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        query = f"SELECT details_clean FROM event_{symbol.lower()} WHERE rowid = ?"
        cursor.execute(query, (rowid,))
        res = cursor.fetchone()
        conn.close()
        return res[0] if res else "Không tìm thấy nội dung."
    except Exception as e:
        return f"Lỗi truy vấn: {e}"

def get_ai_analysis_from_db(symbol, limit):
    """Lấy phân tích từ AI Cache"""
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        query = "SELECT mcp, analysis_result, created_at, pdf_url FROM ai_cache WHERE mcp = ? ORDER BY created_at DESC LIMIT ?"
        cursor.execute(query, (symbol.upper(), limit))
        rows = cursor.fetchall()
        conn.close()
        return rows
    except Exception as e:
        logging.error(f"Lỗi DB AI: {e}")
        return []

# --- 3. HÀM GỬI TIN NHẮN DÀI ---

async def send_smart_message(msg_obj, text, parse_mode='Markdown'):
    MAX_LEN = 4000
    if len(text) <= MAX_LEN:
        try:
            await msg_obj.reply_text(text, parse_mode=parse_mode, disable_web_page_preview=True)
        except Exception:
            # Nếu lỗi định dạng Markdown, gửi ở chế độ văn bản thường
            await msg_obj.reply_text(text, parse_mode=None, disable_web_page_preview=True)
    else:
        for i in range(0, len(text), MAX_LEN):
            part = text[i:i+MAX_LEN]
            try:
                await msg_obj.reply_text(part, parse_mode=parse_mode, disable_web_page_preview=True)
            except Exception:
                await msg_obj.reply_text(part, parse_mode=None, disable_web_page_preview=True)

# --- 4. XỬ LÝ SỰ KIỆN NHẤN NÚT (CALLBACK) ---

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    # Dữ liệu dạng: detail_msn_123
    data = query.data.split("_")
    if data[0] == "detail":
        symbol = data[1]
        rowid = data[2]
        
        content = get_detail_by_rowid(symbol, rowid)
        header = f"📄 **CHI TIẾT TIN TỨC: {symbol.upper()}**\n{'━'*15}\n"
        await send_smart_message(query.message, header + content)

# --- 5. XỬ LÝ TIN NHẮN VĂN BẢN ---

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message or update.channel_post
    if not msg or not msg.text: return

    raw_text = msg.text.strip() # Giữ nguyên chữ hoa chữ thường
    text_lower = raw_text.lower() # Bản này chỉ dùng để kiểm tra lệnh
    
    # Sử dụng Regex để bắt lệnh: phantich_mãcp link
    # Ví dụ: phantich_hdc https://...
    match_ai = re.match(r'^phantich_([a-z0-9]+)\s+(http\S+)', text_lower)
    
    # LỆNH MỚI: phantich https://...
    if match_ai:
        symbol = match_ai.group(1).upper() # Lấy mã CP (ví dụ: HDC)
        # Lấy URL từ bản gốc raw_text để tránh lỗi chữ thường làm hỏng ID Google Drive
        url_match = re.search(r'(http\S+)', raw_text)
        url = url_match.group(1) if url_match else ""
        #url = raw_text[9:].strip()
        if not url:
            await msg.reply_text("❌ Không tìm thấy đường link.")
            return

        # --- BƯỚC KIỂM TRA CACHE TẠI ĐÂY ---
        
        cached_result = get_cached_ai(url)
        
        if cached_result:
            header = header = f"🚀 **KẾT QUẢ ĐÃ LƯU TRƯỚC ĐÓ - {symbol}**\n🔗 {url}\n{'━'*15}\n"
            await send_smart_message(msg, header + cached_result)
            return
        # -----------------------------------

        
        status_msg = await msg.reply_text("🤖 Đang tải và phân tích PDF... Vui lòng đợi.")
        
        result = await analyze_pdf_via_ai(url)
        
        if "❌" not in result:
            # Lưu vào cache với mã CP đã tách được
            save_to_ai_cache(url, symbol, result)
        
        await status_msg.delete() # Xóa câu thông báo "đang đợi"
        header = f"✨ **KẾT QUẢ PHÂN TÍCH AI**\n🔗 {url}\n{'━'*15}\n"
        await send_smart_message(msg, header + result)
        return
    # ... (Giữ nguyên phần xử lý lệnh bctc_ và mã CP cũ bên dưới) ...
    parts = text_lower.lower().split()
    if len(parts) != 2: return

    cmd, limit_str = parts[0], parts[1]
    try:
        limit = int(limit_str)
        
        # TRƯỜNG HỢP: bctc_msn 5 (AI)
        if cmd.startswith("bctc_"):
            symbol = cmd.replace("bctc_", "")
            data = get_ai_analysis_from_db(symbol, limit)
            if not data:
                await msg.reply_text(f"❌ Không có dữ liệu AI cho {symbol.upper()}")
                return
            
            for mcp, result, time, url in data:
                full_ai = f"🤖 **AI ANALYST: {mcp}**\n📅 {time}\n\n{result}\n\n🔗 [Link PDF]({url})"
                await send_smart_message(msg, full_ai)

        # TRƯỜNG HỢP: msn 10 (Tin tức gốc)
        else:
            symbol = cmd
            data = get_data_from_db(symbol, limit)
            if not data:
                await msg.reply_text(f"❌ Không có tin tức cho {symbol.upper()}")
                return

            await msg.reply_text(f"📊 **TIN TỨC GỐC: {symbol.upper()}**")
            for summary, date, detail, rowid in data:
                d = date if (date and date != "None") else "N/A"
                txt = f"*{summary}*\n📅 {d}"
                
                # Tạo nút bấm callback để xem chi tiết
                keyboard = [[InlineKeyboardButton("🔍 Xem Nội Dung", callback_data=f"detail_{symbol}_{rowid}")]]
                await msg.reply_text(txt, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

    except Exception as e:
        logging.error(e)
        await msg.reply_text("⚠️ Lỗi định dạng hoặc hệ thống.")

# --- 6. CHẠY BOT ---

if __name__ == '__main__':
    print("🚀 Bot đang chạy...")
    app = Application.builder().token(TOKEN).build()
    
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_message))
    app.add_handler(CallbackQueryHandler(handle_callback))
    
    app.run_polling()