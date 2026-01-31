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
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import winsound 

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
last_count = 0 

# CẤU HÌNH AI CHUẨN (Đã sửa lỗi 404)
try:
    genai.configure(api_key="AIzaSyBWGkET-D91usOftX82vdNcK9aBo69hNjc")
    AI_MODEL = genai.GenerativeModel('gemini-flash-latest')
    print("AI đã sẵn sàng.")
except Exception as e:
    print(f"Lỗi khởi tạo AI: {e}")

# --- 3. LOGIC XỬ LÝ URL, AI & HIỂN THỊ ---

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
    url_pattern = r'(https?://[^\s\(\)\[\]\{\}\<\>]+)'
    for tag in text_widget.tag_names():
        if tag.startswith("http"): text_widget.tag_delete(tag)
    for match in re.finditer(url_pattern, content):
        start = f"1.0 + {match.start()} chars"
        end = f"1.0 + {match.end()} chars"
        url = match.group(0)
        text_widget.tag_add(url, start, end)
        text_widget.tag_config(url, foreground="#0066CC", underline=True)
        text_widget.tag_bind(url, "<Control-Button-1>", open_url)

def analyze_pdf_with_ai(pdf_url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(pdf_url, headers=headers, timeout=30, verify=False)
        if response.status_code != 200:
            return f"❌ Lỗi tải file: HTTP {response.status_code}"
        
        pdf_blob = response.content
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
    """Trích xuất link PDF thực từ link Google View hoặc AWS"""
    # Nếu link chứa tham số ?url= (đặc trưng của Google GView)
    n = raw_url.find('url=')
    if n != -1:
        clean_url = raw_url[n+4:]
        # Giải mã các ký tự đặc biệt nếu có (ví dụ %3A thành :)
        from urllib.parse import unquote
        return unquote(clean_url)
    return raw_url
def trigger_ai_analysis():
    selected = tree.focus()
    if not selected:
        messagebox.showwarning("Chú ý", "Vui lòng chọn một tin trên bảng!")
        return
    
    item_data = tree.item(selected)
    ticker = item_data['values'][1] # Lấy mã CP
    content = item_data['tags'][-1]
    pdf_match = re.search(r'https?://[^\s]+\.pdf', content)
    
    if not pdf_match:
        messagebox.showinfo("Thông tin", "Tin này không chứa file PDF.")
        return

    raw_url = pdf_match.group(0)
    # SỬ DỤNG HÀM LÀM SẠCH LINK Ở ĐÂY
    pdf_url = clean_pdf_url(raw_url) 
    
    # Kiểm tra xem có đúng là PDF không
    if not pdf_url.lower().endswith('.pdf'):
        messagebox.showinfo("Thông tin", "Link này không dẫn trực tiếp đến file PDF.")
        return
    # Hiển thị thông báo đang xử lý trên giao diện chính
    detail_box.config(state=tk.NORMAL)
    detail_box.insert(tk.END, f"\n\n🤖 ĐANG PHÂN TÍCH AI CHO MÃ {ticker}... Vui lòng đợi cửa sổ mới.")
    detail_box.see(tk.END)
    detail_box.config(state=tk.DISABLED)
    
    def worker():
        result = analyze_pdf_with_ai(pdf_url)
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

# --- 4. TRUY VẤN DỮ LIỆU ---

def fetch_history_data(table_name):
    global current_view_data
    if not table_name: return
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT id, mcp, date, summary, scraped_at, web_source, details_clean FROM {table_name}")
        rows = cursor.fetchall()
        processed_data = []
        for row in rows:
            new_row = list(row)
            if not new_row[2] or new_row[2] == "None":
                new_row[2] = row[4].split(' ')[0] if row[4] else "N/A"
            processed_data.append(tuple(new_row))
        current_view_data = sorted(processed_data, key=lambda x: x[2], reverse=True)
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
            query = f"SELECT id, mcp, date, summary, scraped_at, web_source, details_clean FROM {table}"
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
    return sorted(all_data, key=lambda x: x[2], reverse=True)

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
            query = f"SELECT id, mcp, date, summary, scraped_at, web_source, details_clean FROM {table} WHERE scraped_at LIKE ?"
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
    today_str = date.today().strftime('%Y-%m-%d')
    for item in tree_widget.get_children(): tree_widget.delete(item)
    for row in data:
        summary_text = str(row[3]).lower()
        scraped_at = str(row[4])
        tags = []
        if scraped_at.startswith(today_str): tags.append('new_scraped')
        if "giải thể" in summary_text: tags.append('priority_keyword')
        elif "cổ tức" in summary_text: tags.append('co_tuc')
        
        tags.append(row[6]) 
        tree_widget.insert('', 'end', values=row[:6], tags=tags)

def update_display(mode="today"):
    global current_view_data, last_count
    if mode == "newly":
        current_view_data = get_newly_scraped_data()
        title_prefix = "Mới cập nhật"
    else:
        days = 1 if mode == "today" else 7
        current_view_data = get_filtered_data(days_offset=days)
        title_prefix = 'Hôm nay' if days==1 else '7 ngày qua'
    update_treeview(tree, current_view_data)
    root.title(f"Stock Scraper - {title_prefix} ({len(current_view_data)})")

def on_item_select(event):
    selected = tree.focus()
    if not selected: return
    tags = tree.item(selected, 'tags')
    if tags:
        detail_box.config(state=tk.NORMAL)
        detail_box.delete('1.0', tk.END)
        content = tags[-1] if tags[-1] else "Không có chi tiết."
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
tree.tag_configure('new_scraped', background='#E8F5E9')
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

ai_btn = ttk.Button(ctrl_frame, text="✨ Phân tích AI", command=trigger_ai_analysis)
ai_btn.pack(side='left', padx=5)

# Detail Box
ttk.Label(main_frame, text="Nội dung chi tiết bản tin:", font=('', 9, 'bold')).pack(anchor='w', pady=(10, 0))
detail_box = tk.Text(main_frame, height=30, state=tk.DISABLED, wrap=tk.WORD, bg='#FFFFFF', padx=15, pady=15, font=('Segoe UI', 10))
detail_box.pack(fill='both', expand=True)

tree.bind('<<TreeviewSelect>>', on_item_select)

if __name__ == "__main__":
    update_display("today")
    root.after(300000, auto_refresh) 
    root.mainloop()