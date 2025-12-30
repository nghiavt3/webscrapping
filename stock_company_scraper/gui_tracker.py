import sqlite3
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime, date, timedelta
import os
import threading
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from spider_names import ALL_SPIDERS

# --- CẤU HÌNH ĐƯỜNG DẪN ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_NAME = os.path.join(BASE_DIR, 'stock_events.db')

# --- 1. LOGIC TRUY VẤN DỮ LIỆU ---

def fetch_history_data(table_name):
    """Truy vấn dữ liệu lịch sử từ một bảng cụ thể."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT id, mcp, date, summary, scraped_at, web_source, details_clean FROM {table_name} ORDER BY date DESC")
        return cursor.fetchall()
    except Exception:
        return []
    finally:
        conn.close()

def get_new_events_7days():
    """Truy vấn dữ liệu từ tất cả bảng event_* trong 7 ngày qua."""
    today = (date.today() + timedelta(days=1)).strftime('%Y-%m-%d')
    seven_days_ago = (date.today() - timedelta(days=6)).strftime('%Y-%m-%d')
    
    all_data = []
    if not os.path.exists(DATABASE_NAME):
        return []
        
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'event_%'")
        tables = [row[0] for row in cursor.fetchall()]
        for table in tables:
            query = f"SELECT id, mcp, date, summary, scraped_at, web_source, details_clean FROM {table} WHERE date BETWEEN ? AND ?"
            cursor.execute(query, (seven_days_ago, today))
            all_data.extend(cursor.fetchall())
    except Exception as e:
        print(f"Lỗi truy vấn: {e}")
    finally:
        conn.close()
    return sorted(all_data, key=lambda x: x[2], reverse=True)

# --- 2. LOGIC CHẠY SPIDER SONG SONG (MAX 3) ---

def run_single_spider(spider_name):
    """Thực thi 1 spider qua lệnh hệ thống."""
    try:
        # Sử dụng shell=True trên Windows để chạy scrapy trực tiếp
        process = subprocess.Popen(
            ['scrapy', 'crawl', spider_name],
            cwd=BASE_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            shell=True
        )
        process.communicate() 
        return f"✅ {spider_name} xong"
    except Exception as e:
        return f"❌ {spider_name} lỗi: {e}"

def run_parallel_logic(tree_widget, detail_box, progress_bar, run_btn):
    """Điều phối chạy song song 3 spider cùng lúc."""
    total = len(ALL_SPIDERS)
    completed = 0
    
    # 
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_spider = {executor.submit(run_single_spider, s): s for s in ALL_SPIDERS}
        
        for future in as_completed(future_to_spider):
            completed += 1
            result = future.result()
            
            # Cập nhật GUI an toàn từ Thread
            percentage = (completed / total) * 100
            root.after(0, lambda p=percentage: progress_bar.config(value=p))
            root.after(0, lambda c=completed, t=total: run_btn.config(text=f"⏳ Đang chạy ({c}/{t})..."))

    root.after(0, lambda: finalize_run(tree_widget, detail_box, run_btn))

def finalize_run(tree_widget, detail_box, run_btn):
    run_btn.config(state=tk.NORMAL, text="🚀 Chạy Scrapers (Song song)")
    messagebox.showinfo("Hoàn tất", f"Đã quét xong tất cả {len(ALL_SPIDERS)} nguồn dữ liệu!")
    update_display_7days(tree_widget, detail_box)

# --- 3. GIAO DIỆN GUI ---

def update_treeview(tree_widget, data):
    for item in tree_widget.get_children():
        tree_widget.delete(item)
    for row in data:
        # row: (id, mcp, date, summary, scraped_at, source, details_clean)
        tree_widget.insert('', 'end', values=row[:6], tags=(row[6],))

def update_display_7days(tree_widget, detail_box):
    data = get_new_events_7days()
    update_treeview(tree_widget, data)
    root.title(f"Stock Scraper Pro - {len(data)} Sự kiện mới trong 7 ngày")

def on_item_select(event):
    selected = tree.focus()
    if not selected: return
    tags = tree.item(selected, 'tags')
    content = tags[0] if tags else "Không có chi tiết"
    
    detail_box.config(state=tk.NORMAL)
    detail_box.delete('1.0', tk.END)
    detail_box.insert(tk.END, content)
    detail_box.config(state=tk.DISABLED)

# --- KHỞI TẠO CỬA SỔ CHÍNH ---
root = tk.Tk()
root.title("Stock Scraper Pro")
root.geometry("1150x850") # Đã sửa lỗi syntax ở đây

main_frame = ttk.Frame(root, padding="15")
main_frame.pack(fill='both', expand=True)

# Bảng dữ liệu (Treeview)
cols = ('ID', 'Mã CP', 'Ngày SK', 'Tóm tắt', 'Thời gian Scrape', 'Nguồn')
tree = ttk.Treeview(main_frame, columns=cols, show='headings', height=18)
for c in cols:
    tree.heading(c, text=c, anchor='w')
    tree.column(c, width=100)
tree.column('Tóm tắt', width=450)
tree.pack(fill='x', pady=5)

# Scrollbar cho Treeview
sb = ttk.Scrollbar(main_frame, orient='vertical', command=tree.yview)
tree.configure(yscrollcommand=sb.set)
sb.place(in_=tree, relx=1.0, rely=0, relheight=1.0, anchor='ne')

# Khung điều khiển nút bấm và Progress
ctrl_frame = ttk.Frame(main_frame)
ctrl_frame.pack(fill='x', pady=10)

progress = ttk.Progressbar(ctrl_frame, orient='horizontal', length=250, mode='determinate')
progress.pack(side='left', padx=10)

def handle_start():
    if not ALL_SPIDERS:
        messagebox.showwarning("Cảnh báo", "Danh sách ALL_SPIDERS trống!")
        return
    run_button.config(state=tk.DISABLED)
    progress['value'] = 0
    threading.Thread(target=run_parallel_logic, args=(tree, detail_box, progress, run_button), daemon=True).start()

run_button = ttk.Button(ctrl_frame, text="🚀 Chạy Scrapers (Song song)", command=handle_start)
run_button.pack(side='left', padx=5)

ttk.Button(ctrl_frame, text="🚨 Xem 7 ngày qua", command=lambda: update_display_7days(tree, detail_box)).pack(side='left', padx=5)

# Lựa chọn lịch sử
ttk.Label(ctrl_frame, text=" | Xem bảng:").pack(side='left', padx=5)
combo = ttk.Combobox(ctrl_frame, values=ALL_SPIDERS, state='readonly', width=20)
combo.pack(side='left', padx=5)
combo.bind('<<ComboboxSelected>>', lambda e: update_treeview(tree, fetch_history_data(combo.get())))

# Khung hiển thị chi tiết nội dung
ttk.Label(main_frame, text="Chi tiết nội dung (details_clean):", font=('Arial', 10, 'bold')).pack(anchor='w', pady=(10, 0))
detail_box = tk.Text(main_frame, height=12, state=tk.DISABLED, wrap=tk.WORD, bg='#fcfcfc', padx=10, pady=10)
detail_box.pack(fill='both', expand=True)

# Gán sự kiện chọn dòng
tree.bind('<<TreeviewSelect>>', on_item_select)

if __name__ == "__main__":
    # Tải dữ liệu mặc định khi mở app
    update_display_7days(tree, detail_box)
    root.mainloop()