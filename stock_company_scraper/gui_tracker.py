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
from concurrent.futures import ThreadPoolExecutor, as_completed
import winsound 

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

# --- 2. CẤU HÌNH DỮ LIỆU ---
try:
    from spider_names import ALL_SPIDERS
    ALL_SPIDERS = sorted(ALL_SPIDERS) 
except ImportError:
    ALL_SPIDERS = [] 

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_NAME = os.path.join(BASE_DIR, 'stock_events.db')
current_view_data = [] 
last_count = 0 

# --- 3. LOGIC XỬ LÝ URL & HIỂN THỊ ---

def open_url(event):
    """Mở URL khi người dùng nhấn Ctrl + Click"""
    # Lấy danh sách các tag tại vị trí con trỏ chuột hiện tại
    try:
        tags = detail_box.tag_names(tk.CURRENT)
        for tag in tags:
            if tag.startswith("http"):
                webbrowser.open(tag)
                return
    except Exception as e:
        print(f"Lỗi mở link: {e}")

def highlight_urls(text_widget):
    """Quét văn bản và tạo hyperlink cho các URL"""
    content = text_widget.get("1.0", tk.END)
    # Regex nhận diện URL
    url_pattern = r'(https?://[^\s\(\)\[\]\{\}\<\>]+)'
    
    # Xóa các tag cũ
    for tag in text_widget.tag_names():
        if tag.startswith("http"):
            text_widget.tag_delete(tag)

    for match in re.finditer(url_pattern, content):
        start = f"1.0 + {match.start()} chars"
        end = f"1.0 + {match.end()} chars"
        url = match.group(0)
        
        # Tạo tag mang tên chính URL đó
        text_widget.tag_add(url, start, end)
        text_widget.tag_config(url, foreground="#0066CC", underline=True)
        
        # Bắt các sự kiện cho tag này
        text_widget.tag_bind(url, "<Control-Button-1>", open_url)
        text_widget.tag_bind(url, "<Enter>", lambda e: text_widget.config(cursor="hand2"))
        text_widget.tag_bind(url, "<Leave>", lambda e: text_widget.config(cursor=""))

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
        update_treeview(tree, current_view_data)
        return
    filtered = [row for row in current_view_data if query in str(row[1]).upper()]
    update_treeview(tree, filtered)

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
        row_id = str(row[0]) if row[0] else ""
        summary_text = str(row[3]).lower()
        scraped_at = str(row[4])
        tags = []
        if scraped_at.startswith(today_str): tags.append('new_scraped')
        if "NODATE" in row_id: tags.append('nodate_row')
        else:
            if "giải thể" in summary_text or "thu hồi vốn" in summary_text: tags.append('priority_keyword')
            elif "cổ tức" in summary_text: tags.append('co_tuc')
            elif "chuyển nhượng" in summary_text: tags.append('chuyen_nhuong')
            elif "niêm yết cổ phiếu" in summary_text: tags.append('niem_yet')
            elif "nghị quyết đhđcđ" in summary_text: tags.append('nghi_quyet')
        
        # Tag cuối cùng luôn chứa nội dung Details để hàm on_item_select lấy ra
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
    if mode in ["newly", "today"]: last_count = len(current_view_data)
    update_treeview(tree, current_view_data)
    search_var.set("")
    root.title(f"Stock Scraper - {title_prefix} ({len(current_view_data)})")

def on_item_select(event):
    selected = tree.focus()
    if not selected: return
    tags = tree.item(selected, 'tags')
    if tags:
        detail_box.config(state=tk.NORMAL)
        detail_box.delete('1.0', tk.END)
        
        # Nội dung nằm ở tag cuối cùng
        content = tags[-1] if tags[-1] else "Không có chi tiết."
        detail_box.insert(tk.END, content)
        
        # Quét và tạo Link
        highlight_urls(detail_box)
        
        # Thêm ghi chú hướng dẫn nếu có link
        if "http" in content:
            detail_box.insert(tk.END, "\n\n" + "-"*30)
            detail_box.insert(tk.END, "\n💡 Mẹo: Giữ Ctrl + Click vào đường dẫn màu xanh để mở trình duyệt.")
            
        detail_box.config(state=tk.DISABLED)

def on_combo_confirm(event=None):
    user_input = combo.get().strip().lower()
    if not user_input: return
    if user_input in ALL_SPIDERS:
        fetch_history_data(user_input)
    else:
        matches = [s for s in ALL_SPIDERS if user_input in s]
        if matches:
            combo.set(matches[0])
            fetch_history_data(matches[0])
        else:
            messagebox.showwarning("Không tìm thấy", f"Không tìm thấy nguồn '{user_input}'")

def run_auto_script():
    script_path = os.path.join(BASE_DIR, 'auto_run.py')
    if os.path.exists(script_path):
        subprocess.Popen([sys.executable, script_path], cwd=BASE_DIR, shell=False)
        messagebox.showinfo("Thông báo", "Đã kích hoạt chế độ Auto Run.")
    else:
        messagebox.showwarning("Lỗi", "Không tìm thấy file auto_run.py")

# --- KHỞI TẠO GUI ---
root = tk.Tk()
root.title("Stock Scraper Pro")
root.geometry("1200x850")

main_frame = ttk.Frame(root, padding="15")
main_frame.pack(fill='both', expand=True)

# 1. Top Bar
top_frame = ttk.LabelFrame(main_frame, text="🔍 Công cụ lọc nhanh", padding="10")
top_frame.pack(fill='x', pady=(0, 10))

ttk.Label(top_frame, text="Mã CP:").pack(side='left', padx=2)
search_var = tk.StringVar()
search_entry = ttk.Entry(top_frame, textvariable=search_var, width=12)
search_entry.pack(side='left', padx=5)
search_entry.bind('<Return>', lambda e: perform_search())
ttk.Button(top_frame, text="Tìm", command=perform_search).pack(side='left', padx=2)

ttk.Separator(top_frame, orient='vertical').pack(side='left', fill='y', padx=10)
ttk.Button(top_frame, text="📅 Hôm nay", command=lambda: update_display("today")).pack(side='left', padx=2)
ttk.Button(top_frame, text="⚡ Mới cập nhật", command=lambda: update_display("newly")).pack(side='left', padx=2)
ttk.Button(top_frame, text="🗓️ 7 Ngày qua", command=lambda: update_display("week")).pack(side='left', padx=2)

ttk.Label(top_frame, text=" | Nguồn:").pack(side='left', padx=5)
combo = ttk.Combobox(top_frame, values=ALL_SPIDERS, state='normal', width=22)
combo.pack(side='left', padx=2)
combo.bind('<<ComboboxSelected>>', on_combo_confirm)
combo.bind('<Return>', on_combo_confirm)

# 2. Table
tree = ttk.Treeview(main_frame, columns=('ID', 'Mã CP', 'Ngày SK', 'Tóm tắt', 'Scrape lúc', 'Nguồn'), show='headings', height=18)
for c in tree['columns']:
    tree.heading(c, text=c, anchor='w')
    tree.column(c, width=100)
tree.column('Tóm tắt', width=450)
tree.tag_configure('new_scraped', background='#E8F5E9')
tree.tag_configure('nodate_row', background='#F5F5F5', foreground='#9E9E9E')
tree.tag_configure('co_tuc', background='#E1F5FE', foreground='#01579B')
tree.tag_configure('chuyen_nhuong', background='#FFF3E0', foreground='#E65100')
tree.tag_configure('priority_keyword', background='#FFF9C4', foreground='#D32F2F', font=('', 9, 'bold'))
tree.pack(fill='x', pady=5)

# 3. Control Box
ctrl_frame = ttk.LabelFrame(main_frame, text="⚙️ Hệ thống Scraper", padding="10")
ctrl_frame.pack(fill='x', pady=5)

ttk.Label(ctrl_frame, text="Số luồng:").pack(side='left', padx=5)
worker_combo = ttk.Combobox(ctrl_frame, values=["1", "3", "5", "10"], state='readonly', width=5)
worker_combo.set("3")
worker_combo.pack(side='left', padx=5)

progress = ttk.Progressbar(ctrl_frame, length=250, mode='determinate')
progress.pack(side='left', padx=20)

run_btn = ttk.Button(ctrl_frame, text="🚀 Chạy Scrapers", command=lambda: [run_btn.config(state=tk.DISABLED), threading.Thread(target=run_parallel_logic, args=(progress, run_btn), daemon=True).start()])
run_btn.pack(side='left', padx=5)

auto_run_btn = ttk.Button(ctrl_frame, text="🤖 Auto Run", command=run_auto_script)
auto_run_btn.pack(side='left', padx=5)

# 4. Detail Box (Nơi hiển thị nội dung và Link)
ttk.Label(main_frame, text="Nội dung chi tiết bản tin:", font=('', 9, 'bold')).pack(anchor='w', pady=(10, 0))
detail_box = tk.Text(main_frame, height=12, state=tk.DISABLED, wrap=tk.WORD, 
                     bg='#FFFFFF', padx=15, pady=15, font=('Segoe UI', 10),
                     undo=True)
detail_box.pack(fill='both', expand=True)

tree.bind('<<TreeviewSelect>>', on_item_select)

if __name__ == "__main__":
    update_display("today")
    root.after(300000, auto_refresh) 
    root.mainloop()