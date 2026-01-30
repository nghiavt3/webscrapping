import sqlite3
import tkinter as tk
import sys
import hashlib
import time
from tkinter import ttk, messagebox
from datetime import datetime, date, timedelta
import os
import threading
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
import winsound  # Thêm thư viện phát âm thanh trên Windows

# --- 1. KIỂM TRA QUYỀN TRUY CẬP (DYNAMIC TOKEN) ---
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
last_count = 0  # Biến lưu trữ số lượng tin để so sánh phát âm thanh

# --- 3. LOGIC TRUY VẤN & XỬ LÝ ---

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
            
        # Sắp xếp lại danh sách đã xử lý
        current_view_data = sorted(processed_data, key=lambda x: x[2], reverse=True)
        
        update_treeview(tree, current_view_data)
        root.title(f"Stock Scraper - Bảng {table_name} ({len(current_view_data)} bản ghi)")
    except Exception as e:
        messagebox.showerror("Lỗi", f"Không thể đọc bảng {table_name}: {e}")
    finally:
        conn.close()

def get_filtered_data(days_offset=None):
    today_dt = date.today()
    # Tính mốc thời gian bắt đầu (VD: 7 ngày trước)
    limit_date = today_dt - timedelta(days=days_offset-1) if days_offset else today_dt
    
    all_data = []
    if not os.path.exists(DATABASE_NAME): return []
    
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'event_%'")
        tables = [row[0] for row in cursor.fetchall()]
        
        for table in tables:
            # Lấy tất cả dữ liệu (hoặc có thể giới hạn 1 tháng gần đây để tối ưu hiệu năng)
            query = f"SELECT id, mcp, date, summary, scraped_at, web_source, details_clean FROM {table}"
            cursor.execute(query)
            rows = cursor.fetchall()
            
            for row in rows:
                # Logic quan trọng: Nếu date (row[2]) là None thì dùng scraped_at (row[4])
                raw_date_str = row[2] if row[2] and row[2] != "None" else row[4]
                
                if raw_date_str:
                    try:
                        # Chỉ lấy phần YYYY-MM-DD từ chuỗi ngày (phòng trường hợp scraped_at có giờ)
                        clean_date_str = raw_date_str.split(' ')[0]
                        record_date = datetime.strptime(clean_date_str, '%Y-%m-%d').date()
                        
                        # Kiểm tra xem record_date có nằm trong khoảng mong muốn không
                        if record_date >= limit_date:
                            # Tạo bản ghi mới để hiển thị, thay thế giá trị None bằng ngày scraped_at
                            new_row = list(row)
                            new_row[2] = clean_date_str # Cập nhật cột Date hiển thị
                            all_data.append(tuple(new_row))
                    except:
                        continue
    finally: 
        conn.close()
    
    # Sắp xếp theo ngày (cột index 2) giảm dần
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
            # Lọc trực tiếp bằng SQL theo cột scraped_at
            query = f"SELECT id, mcp, date, summary, scraped_at, web_source, details_clean FROM {table} WHERE scraped_at LIKE ?"
            cursor.execute(query, (f"{today_str}%",))
            rows = cursor.fetchall()
            
            for row in rows:
                new_row = list(row)
                if not new_row[2] or new_row[2] == "None":
                    new_row[2] = row[4].split(' ')[0]
                all_data.append(tuple(new_row))
    finally:
        conn.close()
    
    # Sắp xếp theo thời gian Scrape mới nhất lên đầu (cột index 4)
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
    try:
        max_parallel = int(worker_combo.get())
    except:
        max_parallel = 3

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

# --- NÂNG CẤP: HÀM TỰ ĐỘNG CẬP NHẬT VÀ BÁO ÂM THANH ---
def auto_refresh():
    global last_count
    # Lấy dữ liệu mới nhất của hôm nay để kiểm tra
    data_today = get_filtered_data(days_offset=1)
    current_count = len(data_today)

    # Nếu số lượng tin hôm nay tăng lên so với lần cuối kiểm tra
    if current_count > last_count and last_count != 0:
        # Phát tiếng Ting (tần số 1000Hz, kéo dài 500ms)
        winsound.Beep(1000, 500)
        # Cập nhật lại giao diện để hiển thị tin mới
        update_display("today")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Đã cập nhật {current_count - last_count} tin mới!")
    
    # Cập nhật số lượng mới nhất vào bộ nhớ
    last_count = current_count
    
    # Lập lịch chạy lại sau 5 phút (300,000 ms)
    root.after(300000, auto_refresh)

# --- 4. GIAO DIỆN GUI ---

def update_treeview(tree_widget, data):
    today_str = date.today().strftime('%Y-%m-%d')
    for item in tree_widget.get_children(): tree_widget.delete(item)
    for row in data:
        # 1. Định nghĩa row_id từ cột đầu tiên (index 0) của row
        row_id = str(row[0]) if row[0] else ""
        summary_text = str(row[3]).lower()
        scraped_at = str(row[4])
        tags = []
        if scraped_at.startswith(today_str):
            tags.append('new_scraped')
        if "NODATE" in row_id:
            tags.append('nodate_row')
        else:
            # Chỉ gán màu theo loại tin nếu không phải là dòng NODATE
            if "giải thể" in summary_text or "thu hồi vốn" in summary_text:
                tags.append('priority_keyword')
            elif "cổ tức" in summary_text: tags.append('co_tuc')
            elif "chuyển nhượng" in summary_text: tags.append('chuyen_nhuong')
            elif "niêm yết cổ phiếu" in summary_text: tags.append('niem_yet')
            elif "nghị quyết đhđcđ" in summary_text: tags.append('nghi_quyet')
        
        tags.append(row[6]) 
        tree_widget.insert('', 'end', values=row[:6], tags=tags)

def update_display(mode="today"):
    global current_view_data, last_count
    
    if mode == "newly":
        current_view_data = get_newly_scraped_data()
        title_prefix = "Mới cập nhật hôm nay"
    else:
        days = 1 if mode == "today" else 7
        current_view_data = get_filtered_data(days_offset=days)
        title_prefix = 'Hôm nay' if days==1 else '7 ngày qua'

    # Đồng bộ số lượng để báo âm thanh
    if mode == "newly" or mode == "today":
        last_count = len(current_view_data)
        
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
        detail_box.insert(tk.END, tags[-1])
        detail_box.config(state=tk.DISABLED)

def on_combo_confirm(event=None):
    user_input = combo.get().strip().lower()
    if not user_input: return
    if user_input in ALL_SPIDERS:
        fetch_history_data(user_input)
        return
    target_table = f"event_{user_input}"
    if target_table in ALL_SPIDERS:
        combo.set(target_table)
        fetch_history_data(target_table)
    else:
        matches = [s for s in ALL_SPIDERS if user_input in s]
        if matches:
            combo.set(matches[0])
            fetch_history_data(matches[0])
        else:
            messagebox.showwarning("Không tìm thấy", f"Không tìm thấy bảng nào khớp với '{user_input}'")

def run_auto_script():
    script_path = os.path.join(BASE_DIR, 'auto_run.py')
    if os.path.exists(script_path):
        try:
            # Sử dụng Popen để chạy script độc lập, không làm treo giao diện GUI
            subprocess.Popen([sys.executable, script_path], cwd=BASE_DIR, shell=False)
            messagebox.showinfo("Thông báo", "Đã kích hoạt chế độ Auto Run (Chạy ngầm).")
        except Exception as e:
            messagebox.showerror("Lỗi", f"Không thể khởi chạy auto_run.py: {e}")
    else:
        messagebox.showwarning("Lỗi", "Không tìm thấy file auto_run.py trong thư mục!")
# KHỞI TẠO CỬA SỔ
root = tk.Tk()
root.title("Stock Scraper Pro")
root.geometry("1200x850")

main_frame = ttk.Frame(root, padding="15")
main_frame.pack(fill='both', expand=True)

# 1. Khu vực Bộ lọc & Tìm kiếm
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

ttk.Label(top_frame, text=" | Tìm mã nguồn:").pack(side='left', padx=5)
combo = ttk.Combobox(top_frame, values=ALL_SPIDERS, state='normal', width=22)
combo.pack(side='left', padx=2)
combo.bind('<<ComboboxSelected>>', on_combo_confirm)
combo.bind('<Return>', on_combo_confirm)

# 2. Bảng hiển thị Treeview
tree = ttk.Treeview(main_frame, columns=('ID', 'Mã CP', 'Ngày SK', 'Tóm tắt', 'Scrape lúc', 'Nguồn'), show='headings', height=18)
for c in tree['columns']:
    tree.heading(c, text=c, anchor='w')
    tree.column(c, width=100)
tree.column('Tóm tắt', width=450)
tree.tag_configure('new_scraped', background='#E8F5E9')
tree.tag_configure('nodate_row', background='#F5F5F5', foreground='#9E9E9E') # Màu xám nhạt
tree.tag_configure('co_tuc', background='#E1F5FE', foreground='#01579B')
tree.tag_configure('chuyen_nhuong', background='#FFF3E0', foreground='#E65100')
tree.tag_configure('niem_yet', background='#E8F5E9', foreground='#2E7D32')
tree.tag_configure('nghi_quyet', background='#F3E5F5', foreground='#7B1FA2')
# Thêm dòng này vào khu vực cấu hình tags của Treeview
tree.tag_configure('priority_keyword', background='#FFF9C4', foreground='#D32F2F', font=('', 9, 'bold'))
tree.pack(fill='x', pady=5)

# 3. Khu vực điều khiển
ctrl_frame = ttk.LabelFrame(main_frame, text="⚙️ Hệ thống Scraper", padding="10")
ctrl_frame.pack(fill='x', pady=5)

ttk.Label(ctrl_frame, text="Số luồng chạy song song:").pack(side='left', padx=5)
worker_combo = ttk.Combobox(ctrl_frame, values=["1", "2", "3", "4", "5", "7", "10"], state='readonly', width=5)
worker_combo.set("3")
worker_combo.pack(side='left', padx=5)

progress = ttk.Progressbar(ctrl_frame, length=250, mode='determinate')
progress.pack(side='left', padx=20)

run_btn = ttk.Button(ctrl_frame, text="🚀 Chạy Scrapers", command=lambda: [run_btn.config(state=tk.DISABLED), threading.Thread(target=run_parallel_logic, args=(progress, run_btn), daemon=True).start()])
run_btn.pack(side='left', padx=5)
# --- THÊM NÚT AUTO RUN VÀO ĐÂY ---
auto_run_btn = ttk.Button(ctrl_frame, text="🤖 Auto Run", command=run_auto_script)
auto_run_btn.pack(side='left', padx=5)
# 4. Box nội dung chi tiết
ttk.Label(main_frame, text="Nội dung chi tiết bản tin:", font=('', 9, 'bold')).pack(anchor='w', pady=(10, 0))
detail_box = tk.Text(main_frame, height=12, state=tk.DISABLED, wrap=tk.WORD, bg='#FCFCFC', padx=15, pady=15, font=('Segoe UI', 10))
detail_box.pack(fill='both', expand=True)

tree.bind('<<TreeviewSelect>>', on_item_select)

if __name__ == "__main__":
    update_display("today")
    
    # Bắt đầu vòng lặp tự động cập nhật sau mỗi 5 phút
    root.after(300000, auto_refresh) 
    
    root.mainloop()