import sqlite3
import tkinter as tk
import sys
import hashlib
import time
import webbrowser
import re
from tkinter import ttk, messagebox
from datetime import datetime, date, timedelta
import os
import threading
import subprocess
import requests
import fitz
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import winsound 
from urllib.parse import unquote, quote
# --- THƯ VIỆN BỔ SUNG ---
try:
    import google.generativeai as genai
except ImportError:
    pass

# --- 1. KIỂM TRA QUYỀN TRUY CẬP ---
def check_access():
    if len(sys.argv) < 2:
        sys.exit("Truy cập bị chặn! Vui lòng khởi động từ App Web.")

    received_token = sys.argv[1]
    valid_tokens = []
    for offset in [0, -1]:
        t_str = (datetime.now() + timedelta(minutes=offset)).strftime('%Y-%m-%d %H:%M')
        raw = f"MySecretKey_{t_str}"
        valid_tokens.append(hashlib.sha256(raw.encode()).hexdigest())

    if received_token not in valid_tokens:
        root_auth = tk.Tk()
        root_auth.withdraw()
        messagebox.showerror("Lỗi bảo mật", "Token hết hạn hoặc không hợp lệ!")
        root_auth.destroy()
        sys.exit()

check_access()

# --- 2. CẤU HÌNH DỮ LIỆU & AI ---
try:
    from spider_names import ALL_SPIDERS
    ALL_SPIDERS = sorted(ALL_SPIDERS) 
except ImportError:
    ALL_SPIDERS = [] 

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_NAME = os.path.join(BASE_DIR, 'stock_events.db')
current_view_data = [] 
item_map = {}
last_count = 0 

# CẤU HÌNH AI CHUẨN (Đã sửa lỗi 404)
try:
    genai.configure(api_key="AIzaSyCaGKXaOKGFRq73Qh-psbglhTkCkxpkpPw")
    AI_MODEL = genai.GenerativeModel('gemini-flash-latest')
    print("AI đã sẵn sàng.")
except Exception as e:
    print(f"Lỗi khởi tạo AI: {e}")

# --- 3. LOGIC XỬ LÝ URL, AI & HIỂN THỊ ---

def init_ai_cache_table():
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    # Tạo bảng lưu trữ AI nếu chưa có
    # pdf_url là PRIMARY KEY để đảm bảo không lưu trùng
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ai_cache (
            pdf_url TEXT PRIMARY KEY,
            mcp TEXT,
            analysis_result TEXT,
            sentiment_score INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    try:
        #cursor.execute("ALTER TABLE ai_cache ADD COLUMN mcp TEXT")
        cursor.execute("ALTER TABLE ai_cache ADD COLUMN sentiment_score INTEGER")
        print("thêm column sentiment_score.")
    except sqlite3.OperationalError:
        pass # Cột đã tồn tại
    conn.commit()
    conn.close()

init_ai_cache_table()
def get_cached_ai(pdf_url):
    """Lấy kết quả từ bảng cache dựa trên link PDF"""
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        cursor.execute("SELECT analysis_result FROM ai_cache WHERE pdf_url = ?", (pdf_url,))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else None
    except: return None

def save_ai_to_cache(pdf_url,mcp,score, result_text):
    """Lưu kết quả phân tích mới vào bảng cache"""
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO ai_cache (pdf_url,mcp,sentiment_score, analysis_result) VALUES (?, ?, ?, ?)", 
                       (pdf_url,mcp,score, result_text))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Lỗi lưu cache: {e}")

def open_url(event):
    try:
        tags = event.widget.tag_names(tk.CURRENT)
        for tag in tags:
            if tag.startswith("http"):
                webbrowser.open(tag)
                return
    except Exception as e:
        print(f"Lỗi mở link: {e}")

def highlight_urls(text_widget):
    content = text_widget.get("1.0", tk.END)
    # Regex mới: Bắt đầu bằng http và lấy tất cả ký tự cho đến khi gặp xuống dòng hoặc dấu ngoặc kép
    url_pattern = r'(https?://[^\s"\'\n]+(?:%20|[^\s"\'\n])*|https?://[^\n"\'<>]+)'
    
    for tag in text_widget.tag_names():
        if tag.startswith("http"): 
            text_widget.tag_delete(tag)
            
    for match in re.finditer(url_pattern, content):
        start = f"1.0 + {match.start()} chars"
        end = f"1.0 + {match.end()} chars"
        url = match.group(0).strip() # Xóa khoảng trắng thừa ở đầu/cuối nếu có
        
        # QUAN TRỌNG: Mã hóa link để đảm bảo Ctrl+Click luôn đúng
        from urllib.parse import quote, unquote
        # Giải mã trước rồi mã hóa lại để tránh double-encoding
        safe_url = quote(unquote(url), safe=':/?&=#+')
        
        text_widget.tag_add(safe_url, start, end)
        text_widget.tag_config(safe_url, foreground="#0066CC", underline=True)
        text_widget.tag_bind(safe_url, "<Control-Button-1>", open_url)

def analyze_pdf_with_ai(pdf_url):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        # Tạo session để giữ cookie (quan trọng với Google Drive)
        session = requests.Session()
        response = session.get(pdf_url, headers=headers, timeout=45, verify=False)
        # Xử lý xác nhận virus của Google Drive (nếu file hơi nặng)
        if 'confirm=' in response.text and 'drive.google.com' in pdf_url:
            confirm_match = re.search(r'confirm=([a-zA-Z0-9_-]+)', response.text)
            if confirm_match:
                confirm_token = confirm_match.group(1)
                pdf_url = pdf_url + "&confirm=" + confirm_token
                response = session.get(pdf_url, headers=headers, timeout=45, verify=False)
        if response.status_code != 200:
            return f"❌ Lỗi tải file: HTTP {response.status_code}"
        
        pdf_blob = response.content
        
        if b"%PDF" not in pdf_blob[:1024]: # Kiểm tra trong 1KB đầu tiên
            # Debug: In ra 100 ký tự đầu để xem nó là gì (có thể là HTML lỗi)
            print(f"Nội dung lỗi: {pdf_blob[:100]}")
            return "❌ Lỗi: Link Google Drive này yêu cầu quyền truy cập hoặc không cho phép tải trực tiếp."
        prompt = """
        Hãy phân tích file PDF đính kèm (có thể là văn bản scan):
        1. Tóm tắt 3 nội dung quan trọng nhất ảnh hưởng đến doanh nghiệp.
        2. Đánh giá tác động đến giá cổ phiếu: Tích cực, Tiêu cực hay Trung tính?
        3. Chấm điểm mức độ ảnh hưởng: Từ -10 (Rất xấu) đến +10 (Rất tốt).
        4. So sánh với dữ liệu cùng kỳ và định giá theo p/b ,p/e
        Yêu cầu trả lời bằng tiếng Việt, ngắn gọn và trực diện.
        """
        response_ai = AI_MODEL.generate_content([
            prompt,
            {"mime_type": "application/pdf", "data": pdf_blob}
        ])
        return response_ai.text
    except Exception as e:
        return f"❌ Lỗi AI: {str(e)}"
def clean_pdf_url(raw_url):
    """Chuyển đổi link Google Drive sang link stream trực tiếp"""
    # 1. Làm sạch sơ bộ
    raw_url = unquote(raw_url.strip())
    
    # 2. Xử lý link Google Drive
    if 'drive.google.com' in raw_url:
        file_id_match = re.search(r'/d/([a-zA-Z0-9_-]+)', raw_url)
        if file_id_match:
            file_id = file_id_match.group(1)
            # Sử dụng link export hoặc uc
            return f"https://drive.google.com/uc?export=download&id={file_id}"

    # 3. Xử lý link từ redirect (nếu có)
    n = raw_url.find('url=')
    if n != -1:
        raw_url = unquote(raw_url[n+4:])
    
    return raw_url

def trigger_ai_analysis():
    selected = tree.focus()
    if not selected:
        messagebox.showwarning("Chú ý", "Vui lòng chọn một tin trên bảng!")
        return
    raw_url = None # Khởi tạo giá trị mặc định
    try:
        # Kiểm tra xem người dùng có đang bôi đen (selection) văn bản không
        raw_url = detail_box.get(tk.SEL_FIRST, tk.SEL_LAST).strip()
    except tk.TclError:
        pass # Không có vùng chọn
    # 2. Nếu không bôi đen, tự động tìm link trong nội dung bản tin
    if not raw_url:
        item_data = tree.item(selected)
        #ticker = item_data['values'][1] # Lấy mã CP
        content = item_map.get(selected, "")
        pdf_match = re.search(r'(https?://[^\s<>"]+)', content)
        if pdf_match:
            raw_url = pdf_match.group(0)

    # 3. Kiểm tra xem cuối cùng có lấy được link nào không
    if not raw_url:
        messagebox.showinfo("Thông tin", "Vui lòng bôi đen đường link cụ thể hoặc chọn tin có liên kết!")
        return
    

    # SỬ DỤNG HÀM LÀM SẠCH LINK Ở ĐÂY
    pdf_url = clean_pdf_url(raw_url) 
    
    is_google_drive = "drive.google.com" in pdf_url
    #is_direct_pdf = pdf_url.lower().split('?')[0].endswith('.pdf')
    is_direct_pdf =".pdf" in pdf_url.lower()
    is_export = "uc?export=" in pdf_url.lower()
    
    if not (is_google_drive or is_direct_pdf or is_export):
        messagebox.showinfo("Thông tin", "Link này không nhận diện được định dạng PDF.")
        return
    ticker = tree.item(selected)['values'][1]
    # --- BƯỚC KIỂM TRA CACHE TẠI BẢNG RIÊNG ---
    cached_result = get_cached_ai(pdf_url)
    if cached_result:
        print(f"🚀 Tìm thấy cache cho PDF: {pdf_url}")
        display_ai_popup(ticker, f"[KẾT QUẢ ĐÃ LƯU TRƯỚC ĐÓ]\n\n{cached_result}", pdf_url)
        return
    # Hiển thị thông báo đang xử lý trên giao diện chính
    detail_box.config(state=tk.NORMAL)
    detail_box.insert(tk.END, f"\n\n🤖 ĐANG PHÂN TÍCH AI CHO MÃ {ticker}... Vui lòng đợi cửa sổ mới.")
    detail_box.see(tk.END)
    detail_box.config(state=tk.DISABLED)
    
    def worker():
        result = analyze_pdf_with_ai(pdf_url)
        if "❌" not in result:
            # Trích xuất điểm số từ nội dung AI trả về (để lưu vào cột riêng nếu cần)
            score_match = re.search(r"(-?\d+)", result)
            score = int(score_match.group(1)) if score_match else 0
            save_ai_to_cache(pdf_url,ticker,score, result) # Lưu vào bảng ai_cache
        root.after(0, lambda: display_ai_popup(ticker, result, pdf_url))

    threading.Thread(target=worker, daemon=True).start()

def display_ai_popup(ticker, result, url):
    """Tạo một cửa sổ Popup mới để hiển thị kết quả AI"""
    popup = tk.Toplevel(root)
    popup.title(f"AI Analyst - Mã: {ticker}")
    popup.geometry("700x550")
    popup.configure(bg="#F0F2F5")

    # Header
    header = tk.Label(popup, text=f"BÁO CÁO PHÂN TÍCH AI - {ticker}", 
                      font=('Segoe UI', 14, 'bold'), bg="#F0F2F5", fg="#1A73E8")
    header.pack(pady=10)

    # Text Area
    text_frame = ttk.Frame(popup)
    text_frame.pack(fill='both', expand=True, padx=20, pady=5)
    
    ai_box = tk.Text(text_frame, wrap=tk.WORD, font=('Segoe UI', 11), 
                     padx=15, pady=15, bg="white", borderwidth=0)
    ai_box.pack(side='left', fill='both', expand=True)
    
    scrollbar = ttk.Scrollbar(text_frame, command=ai_box.yview)
    scrollbar.pack(side='right', fill='y')
    ai_box.config(yscrollcommand=scrollbar.set)

    # Chèn dữ liệu
    ai_box.insert(tk.END, f"Nguồn file: {url}\n")
    ai_box.insert(tk.END, "-"*50 + "\n\n")
    ai_box.insert(tk.END, result)
    
    highlight_urls(ai_box)
    ai_box.config(state=tk.DISABLED)

    # Footer
    btn_close = ttk.Button(popup, text="Đóng", command=popup.destroy)
    btn_close.pack(pady=10)

def get_text_from_pdf_url(pdf_url):
    """Tải file PDF từ URL và trích xuất văn bản"""
    try:
        response = requests.get(pdf_url, timeout=15)
        if response.status_code == 200:
            with fitz.open(stream=BytesIO(response.content), filetype="pdf") as doc:
                text = ""
                # Chỉ lấy 3-5 trang đầu để tránh quá tải AI và tiết kiệm token
                for page in doc[:5]:
                    text += page.get_text()
                return text
        return None
    except Exception as e:
        print(f"Lỗi đọc PDF: {e}")
        return None
def analyze_market_impact(pdf_url, summary_fallback):
    """Logic phân tích: Check Cache -> Read PDF -> AI Analyze -> Save Cache"""
    cached_result = get_cached_ai(pdf_url)
    
    if cached_result:
        print(f"🚀 Tìm thấy cache cho PDF: {pdf_url}")
        #display_ai_popup('LUATVIETNAM', f"[KẾT QUẢ ĐÃ LƯU TRƯỚC ĐÓ]\n\n{cached_result}", pdf_url)
        return f"[DỮ LIỆU TỪ CACHE]\n{cached_result}"

    # 2. Nếu chưa có, tiến hành đọc nội dung PDF
    pdf_text = get_text_from_pdf_url(pdf_url)
    
    # Nếu không đọc được PDF, dùng tiêu đề (summary) để phân tích tạm thời
    input_content = pdf_text if pdf_text and len(pdf_text) > 100 else summary_fallback

    prompt = f"""
    Bạn là chuyên gia phân tích chính sách kinh tế. Hãy đọc nội dung văn bản sau và đánh giá tác động đến TTCK Việt Nam:
    Nội dung: {input_content[:4000]} 
    
    Yêu cầu xuất ra định dạng sau:
    - TÓM TẮT: (1-2 câu chính yếu)
    - NHÓM NGÀNH HƯỞNG LỢI:
    - NHÓM NGÀNH RỦI RO:
    - ĐIỂM TÁC ĐỘNG: (Từ -10 đến +10)
    - CHIẾN LƯỢC: (Mua/Bán/Theo dõi)
    """

    try:
        response = AI_MODEL.generate_content(prompt)
        analysis_text = response.text
        
        # Trích xuất điểm số từ nội dung AI trả về (để lưu vào cột riêng nếu cần)
        score_match = re.search(r"(-?\d+)", analysis_text)
        score = int(score_match.group(1)) if score_match else 0

        
        # 3. Lưu vào Cache
        save_ai_to_cache(pdf_url,'LUATVIETNAM',score, analysis_text)
        return analysis_text
    except Exception as e:
        return f"⚠️ Lỗi AI: {str(e)}"
def run_auto_impact_assessment():
    selected = tree.focus()
    
    if not selected:
        messagebox.showinfo("Chú ý", "Vui lòng chọn một dòng dữ liệu.")
        return
    raw_url = None # Khởi tạo giá trị mặc định
    try:
        # Kiểm tra xem người dùng có đang bôi đen (selection) văn bản không
        raw_url = detail_box.get(tk.SEL_FIRST, tk.SEL_LAST).strip()
    except tk.TclError:
        pass # Không có vùng chọn
    if not raw_url:
        item_data = tree.item(selected)
        #ticker = item_data['values'][1] # Lấy mã CP
        content = item_map.get(selected, "")
        pdf_match = re.search(r'(https?://[^\s<>"]+)', content)
        if pdf_match:
            raw_url = pdf_match.group(0)

        # 3. Kiểm tra xem cuối cùng có lấy được link nào không
    if not raw_url:
        messagebox.showinfo("Thông tin", "Vui lòng bôi đen đường link cụ thể hoặc chọn tin có liên kết!")
        return
    
    # Giả định cột chứa URL download là cột cuối cùng hoặc dựa trên logic scraper của bạn
    # Bạn cần đảm bảo details_raw chứa URL PDF
    summary = item_data['values'][3]
    ticker = item_data['values'][1]
    
    if ticker != "LUATVIETNAM":
        messagebox.showwarning("Chú ý", "Chức năng này tối ưu cho dữ liệu văn bản pháp luật.")
        return
    # Tìm link PDF trong chuỗi details_raw bằng Regex
    #pdf_links = re.findall(r'(https?://[^\s]', raw_url)
    pdf_url = raw_url if raw_url else None

    if not pdf_url:
        messagebox.showwarning("Thiếu dữ liệu", "Không tìm thấy link PDF để phân tích chuyên sâu.")
        return

    # Hiển thị cửa sổ kết quả
    popup = tk.Toplevel(root)
    popup.title("Phân tích tác động AI (Deep Analysis)")
    popup.geometry("600x500")
    
    txt = tk.Text(popup, wrap=tk.WORD, font=('Segoe UI', 10), padx=15, pady=15)
    txt.pack(fill='both', expand=True)
    txt.insert(tk.END, "🚀 Đang đọc PDF và phân tích chuyên sâu... Vui lòng đợi...")

    def worker():
        res = analyze_market_impact(pdf_url, summary)
        root.after(0, lambda: txt.delete('1.0', tk.END))
        root.after(0, lambda: txt.insert(tk.END, res))

    threading.Thread(target=worker, daemon=True).start()
def show_luatvietnam_only():
    """Chức năng nút mới: Chỉ hiển thị dữ liệu từ bảng event_luatvietnam"""
    global current_view_data
    fetch_history_data("event_luatvietnam")
    root.title("Stock Scraper - Chuyên mục Văn bản Pháp luật (LuatVietNam)")
    
# --- 4. TRUY VẤN DỮ LIỆU ---

def fetch_history_data(table_name):
    global current_view_data
    if not table_name: return
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute(f"""SELECT t.id, t.mcp, t.date, t.summary, t.scraped_at, t.web_source, t.details_clean, c.sentiment_score 
                       FROM {table_name} t LEFT JOIN ai_cache c ON t.details_clean LIKE '%' || c.pdf_url || '%'
                        ORDER BY t.date DESC
                       """)
        rows = cursor.fetchall()
        processed_data = []
        for row in rows:
            new_row = list(row)
            if not new_row[2] or new_row[2] == "None":
                new_row[2] = row[4].split(' ')[0] if row[4] else "N/A"
                
            #processed_data.append(tuple(new_row))
            # Lưu cả dữ liệu và tag vào list (dùng tuple để quản lý)
            processed_data.append(tuple(new_row))
            
        # Sắp xếp theo ngày
        current_view_data = sorted(processed_data, key=lambda x: str(x[2]), reverse=True)
        update_treeview(tree, current_view_data)
        root.title(f"Stock Scraper - {table_name} ({len(current_view_data)} bản ghi)")
    except Exception as e:
        messagebox.showerror("Lỗi", f"Không thể đọc bảng {table_name}: {e}")
    finally:
        conn.close()

def get_filtered_data(days_offset=None):
    today_dt = date.today()
    limit_date = today_dt - timedelta(days=days_offset-1) if days_offset else today_dt
    all_data = []
    if not os.path.exists(DATABASE_NAME): return []
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'event_%'")
        tables = [row[0] for row in cursor.fetchall()]
        for table in tables:
            # THÊM LEFT JOIN VÀO ĐÂY
            query = f"""
                SELECT t.id, t.mcp, t.date, t.summary, t.scraped_at, t.web_source, t.details_clean, c.sentiment_score 
                FROM {table} t 
                LEFT JOIN ai_cache c ON t.details_clean LIKE '%' || c.pdf_url || '%'
            """
            cursor.execute(query)
            for row in cursor.fetchall():
                raw_date_str = row[2] if row[2] and row[2] != "None" else row[4]
                if raw_date_str:
                    try:
                        clean_date_str = raw_date_str.split(' ')[0]
                        record_date = datetime.strptime(clean_date_str, '%Y-%m-%d').date()
                        if record_date >= limit_date:
                            new_row = list(row)
                            new_row[2] = clean_date_str
                            all_data.append(tuple(new_row))
                    except: continue
    finally: conn.close()
    return sorted(all_data, key=lambda x: str(x[2]), reverse=True)

def get_newly_scraped_data():
    today_str = date.today().strftime('%Y-%m-%d')
    all_data = []
    if not os.path.exists(DATABASE_NAME): return []
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'event_%'")
        tables = [row[0] for row in cursor.fetchall()]
        for table in tables:
            # THÊM LEFT JOIN VÀO ĐÂY
            query = f"""
                SELECT t.id, t.mcp, t.date, t.summary, t.scraped_at, t.web_source, t.details_clean, c.sentiment_score 
                FROM {table} t 
                LEFT JOIN ai_cache c ON t.details_clean LIKE '%' || c.pdf_url || '%'
                WHERE t.scraped_at LIKE ?
            """
            cursor.execute(query, (f"{today_str}%",))
            for row in cursor.fetchall():
                new_row = list(row)
                if not new_row[2] or new_row[2] == "None":
                    new_row[2] = row[4].split(' ')[0]
                all_data.append(tuple(new_row))
    finally: conn.close()
    return sorted(all_data, key=lambda x: x[4], reverse=True)

def perform_search():
    query = search_var.get().strip().upper()
    if not query:
        # Nếu để trống ô tìm kiếm, hiển thị lại toàn bộ dữ liệu hiện tại
        update_treeview(tree, current_view_data)
        return
    
    # Lọc dựa trên cột Mã CP (index 1) hoặc nội dung Tóm tắt (index 3)
    filtered = [
        row for row in current_view_data 
        if query in str(row[1]).upper() or query in str(row[3]).upper()
    ]
    
    update_treeview(tree, filtered)
    
    # Cập nhật tiêu đề để biết đang xem kết quả tìm kiếm
    root.title(f"Kết quả tìm kiếm cho: {query} ({len(filtered)} bản ghi)")

def run_parallel_logic(progress_bar, run_btn):
    total = len(ALL_SPIDERS)
    if total == 0: return
    try: max_parallel = int(worker_combo.get())
    except: max_parallel = 3
    completed = 0
    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = {executor.submit(lambda s: subprocess.run(['scrapy', 'crawl', s], shell=True, cwd=BASE_DIR), s): s for s in ALL_SPIDERS}
        for future in as_completed(futures):
            completed += 1
            root.after(0, lambda p=(completed/total)*100: progress_bar.config(value=p))
            root.after(0, lambda c=completed, t=total: run_btn.config(text=f"⏳ ({c}/{t})..."))
    root.after(0, lambda: finalize_run(run_btn))

def finalize_run(run_btn):
    run_btn.config(state=tk.NORMAL, text="🚀 Chạy Scrapers")
    messagebox.showinfo("Xong", "Đã cập nhật dữ liệu mới!")
    update_display("today")

def auto_refresh():
    global last_count
    data_today = get_filtered_data(days_offset=1)
    current_count = len(data_today)
    if current_count > last_count and last_count != 0:
        winsound.Beep(1000, 500)
        update_display("today")
    last_count = current_count
    root.after(300000, auto_refresh)

# --- 5. GIAO DIỆN CHÍNH ---

def update_treeview(tree_widget, data):
    global item_map
    item_map = {} # Reset bản đồ dữ liệu
    today_str = date.today().strftime('%Y-%m-%d')
    
    for item in tree_widget.get_children(): 
        tree_widget.delete(item)
    
    for row in data:
        # row: (id, mcp, date, summary, scraped_at, source, details_clean, score)
        summary_text = str(row[3]).lower()
        scraped_at = str(row[4])
        score = row[7] if (len(row) > 7 and row[7] is not None) else 0
            
        tags = []
        if score > 0: tags.append('positive')
        elif score < 0: tags.append('negative')
        else: tags.append('neutral')

        if scraped_at.startswith(today_str): tags.append('new_scraped')
        if "giải thể" in summary_text: tags.append('priority_keyword')
        elif "cổ tức" in summary_text: tags.append('co_tuc')

        # Chèn vào Treeview và lưu ID dòng
        item_id = tree_widget.insert('', 'end', values=row[:6], tags=tags)
        
        # Lưu nội dung details_clean (index 6) vào item_map với key là item_id
        item_map[item_id] = row[6] if len(row) > 6 else "Không có chi tiết."

def update_display(mode="today"):
    global current_view_data, last_count
    if mode == "newly":
        current_view_data = get_newly_scraped_data()
        title_prefix = "Mới cập nhật"
    elif mode == "5days":
        # Thêm logic lọc 5 ngày ở đây
        current_view_data = get_filtered_data(days_offset=5)
        title_prefix = "5 ngày gần nhất"
    else:
        days = 1 if mode == "today" else 7
        current_view_data = get_filtered_data(days_offset=days)
        title_prefix = 'Hôm nay' if days==1 else '7 ngày qua'
        
    update_treeview(tree, current_view_data)
    root.title(f"Stock Scraper - {title_prefix} ({len(current_view_data)})")

def on_item_select(event):
    selected = tree.focus()
    if not selected: return
    
    # Lấy nội dung từ item_map đã lưu lúc update_treeview
    content = item_map.get(selected, "Không có chi tiết.")
    
    detail_box.config(state=tk.NORMAL)
    detail_box.delete('1.0', tk.END)
    detail_box.insert(tk.END, content)
    highlight_urls(detail_box)
    detail_box.config(state=tk.DISABLED)

def on_combo_confirm(event=None):
    user_input = combo.get().strip().lower() # Chuyển về chữ thường vì tên table là chữ thường
    if not user_input: return
    
    # Danh sách các bảng thực tế (đã có tiền tố event_)
    # 1. Nếu người dùng gõ thẳng 'event_yeg'
    if user_input in ALL_SPIDERS:
        fetch_history_data(user_input)
        return
        
    # 2. Nếu người dùng chỉ gõ 'yeg', ta thử tìm 'event_yeg'
    suggested_table = f"event_{user_input}"
    if suggested_table in ALL_SPIDERS:
        combo.set(suggested_table) # Cập nhật lại tên đầy đủ vào combobox cho đẹp
        fetch_history_data(suggested_table)
        return
    
    # 3. Nếu vẫn không thấy, thông báo cho người dùng
    messagebox.showwarning("Không tìm thấy", f"Không tìm thấy bảng dữ liệu nào liên quan đến '{user_input}'")

def run_auto_script():
    script_path = os.path.join(BASE_DIR, 'auto_run.py')
    if os.path.exists(script_path):
        subprocess.Popen([sys.executable, script_path], cwd=BASE_DIR, shell=False)
        messagebox.showinfo("Thông báo", "Đã kích hoạt chế độ Auto Run.")

# --- KHỞI TẠO GUI ---
root = tk.Tk()
root.title("Stock Scraper Pro")
root.geometry("1200x850")

main_frame = ttk.Frame(root, padding="15")
main_frame.pack(fill='both', expand=True)

# Top Bar
top_frame = ttk.LabelFrame(main_frame, text="🔍 Công cụ lọc nhanh", padding="10")
top_frame.pack(fill='x', pady=(0, 10))

ttk.Label(top_frame, text="Mã CP:").pack(side='left', padx=2)
search_var = tk.StringVar()
search_entry = ttk.Entry(top_frame, textvariable=search_var, width=12)
search_entry.pack(side='left', padx=5)
search_entry.bind('<Return>', lambda e: perform_search())
ttk.Button(top_frame, text="Tìm", command=perform_search).pack(side='left', padx=2)
ttk.Button(top_frame, text="📅 Hôm nay", command=lambda: update_display("today")).pack(side='left', padx=2)

ttk.Button(top_frame, text="⚡ Mới cập nhật", command=lambda: update_display("newly")).pack(side='left', padx=2)
ttk.Button(top_frame, text="📅 5 Ngày", command=lambda: update_display("5days")).pack(side='left', padx=2)
ttk.Button(top_frame, text="⚖️ Luật Việt Nam", command=show_luatvietnam_only).pack(side='left', padx=5) # NÚT MỚI 1
ttk.Label(top_frame, text=" | Nguồn:").pack(side='left', padx=5)
combo = ttk.Combobox(top_frame, values=ALL_SPIDERS, state='normal', width=22)
combo.pack(side='left', padx=2)
combo.bind('<<ComboboxSelected>>', on_combo_confirm)
combo.bind('<Return>', on_combo_confirm) # Thêm dòng này để nhận lệnh khi nhấn Enter
# Table
tree = ttk.Treeview(main_frame, columns=('ID', 'Mã CP', 'Ngày SK', 'Tóm tắt', 'Scrape lúc', 'Nguồn'), show='headings', height=15)
for c in tree['columns']:
    tree.heading(c, text=c, anchor='w')
    tree.column(c, width=100)
tree.column('Tóm tắt', width=450)
tree.tag_configure('neutral', background='#ffffff')
tree.tag_configure('new_scraped', background='#E8F5E9') # Màu xanh lá cực nhạt cho tin mới
tree.tag_configure('co_tuc', foreground='#0000FF')

# Định nghĩa các tag quan trọng (AI) SAU CÙNG để nó đè lên màu tin mới
tree.tag_configure('positive', background='#e6ffed', foreground='#006400') # Xanh lá
tree.tag_configure('negative', background='#fff1f0', foreground='#8b0000') # Đỏ nhạt
tree.tag_configure('priority_keyword', background='#FFF9C4', font=('', 9, 'bold'))

tree.pack(fill='x', pady=5)

# Control Box
ctrl_frame = ttk.LabelFrame(main_frame, text="⚙️ Hệ thống Scraper", padding="10")
ctrl_frame.pack(fill='x', pady=5)

ttk.Label(ctrl_frame, text="Số luồng:").pack(side='left', padx=5)
worker_combo = ttk.Combobox(ctrl_frame, values=["1", "3", "5", "10"], state='readonly', width=5)
worker_combo.set("3")
worker_combo.pack(side='left', padx=5)

progress = ttk.Progressbar(ctrl_frame, length=200, mode='determinate')
progress.pack(side='left', padx=15)

run_btn = ttk.Button(ctrl_frame, text="🚀 Chạy Scrapers", command=lambda: [run_btn.config(state=tk.DISABLED), threading.Thread(target=run_parallel_logic, args=(progress, run_btn), daemon=True).start()])
run_btn.pack(side='left', padx=5)
# NÚT AUTO RUN MỚI THÊM VÀO ĐÂY
auto_btn = ttk.Button(ctrl_frame, text="🤖 Auto Run", command=run_auto_script)
auto_btn.pack(side='left', padx=5)

ai_btn = ttk.Button(ctrl_frame, text="✨ Phân tích AI", command=trigger_ai_analysis)
ai_btn.pack(side='left', padx=5)
impact_btn = ttk.Button(ctrl_frame, text="📈 Đánh giá TTCK", command=run_auto_impact_assessment) # NÚT MỚI 2
impact_btn.pack(side='left', padx=5)
# Detail Box
ttk.Label(main_frame, text="Nội dung chi tiết bản tin:", font=('', 9, 'bold')).pack(anchor='w', pady=(10, 0))
detail_box = tk.Text(main_frame, height=30, state=tk.DISABLED, wrap=tk.WORD, bg='#FFFFFF', padx=15, pady=15, font=('Segoe UI', 10))
detail_box.pack(fill='both', expand=True)

tree.bind('<<TreeviewSelect>>', on_item_select)

if __name__ == "__main__":
    update_display("today")
    root.after(300000, auto_refresh) 
    root.mainloop()