import sqlite3
import tkinter as tk
from tkinter import ttk ,messagebox
from datetime import datetime
import os
import json
from datetime import date, timedelta
import subprocess # Cần thiết để chạy Scrapy
import threading # Cần thiết để chạy subprocess mà không làm treo GUI
from spider_names import ALL_SPIDERS
# --- CẤU HÌNH ---
# Đường dẫn thư mục chứa file gui_tracker.py
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) 
print(f"DEBUG: Đường dẫn cơ sở (BASE_DIR) là: {BASE_DIR}")

# --- CẤU HÌNH ---
# Sử dụng BASE_DIR để tạo đường dẫn tuyệt đối cho các file dữ liệu
DATABASE_NAME = os.path.join(BASE_DIR, 'stock_events.db')
LOG_FILE_NAME = os.path.join(BASE_DIR, 'new_events_today.txt')
# DATABASE_NAME = 'stock_events.db'
# LOG_FILE_NAME = 'new_events_today.txt'

# --- 1. LOGIC DỮ LIỆU ---

def load_new_events_log(log_file=LOG_FILE_NAME):
    """Đọc và trả về danh sách các sự kiện mới từ file log JSON Lines."""
    new_events = []
    if not os.path.exists(log_file):
        print('file không tồn tại')
        return new_events
        
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                print(line)
                if line and line.startswith('{'):
                    # Chỉ đọc các dòng là đối tượng JSON (loại bỏ các dòng header/footer nếu có)
                    try:
                        data = json.loads(line)
                        # Thêm tên nguồn để dễ phân biệt trong GUI
                        if 'download_url' in data: 
                            data['source_tag'] = 'Seaprimexco'
                        elif 'details_clean' in data:
                            data['source_tag'] = 'Vietstock'
                        new_events.append(data)
                    except json.JSONDecodeError:
                        continue
    except Exception as e:
        print(f"Lỗi đọc file log '{log_file}': {e}")
        
    return new_events

def fetch_history_data(table_name):
    """Truy vấn dữ liệu lịch sử từ một bảng SQLite cụ thể."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    try:
        # Chọn các cột cần thiết, sắp xếp theo ngày gần nhất
        cursor.execute(f"SELECT id, mcp, date, summary, scraped_at,web_source, details_clean FROM {table_name} ORDER BY date DESC")
        data = cursor.fetchall()
    except sqlite3.OperationalError as e:
        print(f"Lỗi truy vấn bảng {table_name}: {e}. Có thể bảng chưa tồn tại.")
        data = []
    conn.close()
    return data

# --- 2. LOGIC GUI (HIỂN THỊ) ---

def clear_treeview(tree_widget):
    """Xóa tất cả các hàng hiện có trong Treeview."""
    for item in tree_widget.get_children():
        tree_widget.delete(item)

def on_item_select(event, tree_widget, detail_box):
    """Xử lý sự kiện khi một hàng trong bảng được chọn để hiển thị chi tiết."""
    selected_item = tree_widget.focus()
    if not selected_item:
        return
        
    # Lấy dữ liệu chi tiết (details_clean) đã lưu trữ trong 'tags' khi chèn
    # Tag là một tuple, chi tiết nằm ở phần tử đầu tiên
    item_tags = tree_widget.item(selected_item, 'tags')
    if item_tags:
        details_clean = item_tags[0]
    else:
        details_clean = "Không có thông tin chi tiết (details_clean) cho sự kiện này."
        
    summary = tree_widget.item(selected_item, 'values')[2]

    # Cập nhật Text Box Chi tiết
    detail_box.config(state=tk.NORMAL) 
    detail_box.delete('1.0', tk.END)
    
    formatted_details = f"===== TÓM TẮT: {summary} =====\n\n{details_clean}"
    
    detail_box.insert(tk.END, formatted_details)
    detail_box.config(state=tk.DISABLED) 

def display_history_data(tree_widget, table_name, detail_box):
    """Tải và hiển thị dữ liệu LỊCH SỬ từ SQLite."""
    clear_treeview(tree_widget)
    detail_box.config(state=tk.NORMAL); detail_box.delete('1.0', tk.END); detail_box.config(state=tk.DISABLED)

    history_data = fetch_history_data(table_name)
    
    # Cột hiển thị (Không bao gồm details_clean)
    for row in history_data:
        # row[0]=id, row[1]=mcp, row[2]=date, row[3]=summary, row[4]=scraped_at, row[5]=details_clean
        display_values = (row[0], row[1], row[2], row[3], row[4], row[5])
        details_clean = row[6] # Lấy details_clean cho tag
        
        # Chèn hàng vào Treeview, lưu details_clean vào tags
        tree_widget.insert('', 'end', values=display_values, tags=(details_clean,))
    
    root.title(f"Stock Scraper GUI - Lịch sử: {table_name}")
    print(f"Đã tải {len(history_data)} sự kiện từ bảng {table_name}")


def display_new_events(tree_widget, detail_box):
    """Tải và hiển thị dữ liệu SỰ KIỆN MỚI từ file log."""
    clear_treeview(tree_widget)
    detail_box.config(state=tk.NORMAL); detail_box.delete('1.0', tk.END); detail_box.config(state=tk.DISABLED)

    new_data = load_new_events_log()
    print(new_data);
    for i, data in enumerate(new_data):
        # Dữ liệu hiển thị trong cột Treeview:
        display_values = (
            f"MỚI_{i+1}", 
            data.get('mcp'),
            data.get('date'), 
            data.get('summary'), 
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            data.get('source_tag', 'Log') # Thêm thẻ nguồn
        )
        
        # Lấy details_clean (hoặc summary nếu không có details_clean) để lưu vào tags
        details_clean = data.get('details_clean', data.get('summary', 'N/A'))
        
        # Chèn hàng vào Treeview, lưu details_clean vào tags
        tree_widget.insert('', 'end', values=display_values, tags=(details_clean,), iid=f"new_{i}", open=True)

    root.title("Stock Scraper GUI - 🚨 SỰ KIỆN MỚI TRONG NGÀY")
    print(f"Đã tải {len(new_data)} sự kiện MỚI từ file log")

def display_new_events_7days(tree_widget, detail_box):
    """
    Hiển thị các sự kiện mới xảy ra trong vòng 7 ngày trước.
    """
    # 1. Định nghĩa khoảng thời gian 7 ngày TRƯỚC
    today = date.today()+ timedelta(days=1)
    seven_days_ago = today - timedelta(days=7)

    # 2. Định dạng ngày tháng cho truy vấn SQL
    # CHÚ Ý: SQLITE hoạt động tốt nhất khi so sánh ngày ở định dạng 'YYYY-MM-DD'
    start_date_str = seven_days_ago.strftime('%Y-%m-%d')
    end_date_str = today.strftime('%Y-%m-%d')
    
    # 3. Xóa dữ liệu cũ
    tree_widget.delete(*tree_widget.get_children())
    detail_box.config(state='normal')
    detail_box.delete(1.0, 'end')
    detail_box.config(state='disabled')

    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        
        # Lấy danh sách tất cả các bảng sự kiện
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'event_%'")
        tables = [row[0] for row in cursor.fetchall()]
        
        # 4. Lặp qua từng bảng và truy vấn
        for table_name in tables:
            # Truy vấn: Lấy các sự kiện có ngày nằm giữa 7 ngày trước và Hôm nay
            # Lưu ý: Cột date trong SELECT phải đủ 6 cột như đã sửa trước đó.
            
            # --- KIỂM TRA ĐỊNH DẠNG NGÀY TRONG DATABASE ---
            # PHƯƠNG ÁN TỐT NHẤT (Nếu date trong DB là YYYY-MM-DD):
            query = f"""
                SELECT id, mcp, date, summary, scraped_at, web_source , details_clean 
                FROM {table_name} 
                WHERE date BETWEEN ? AND ? 
                ORDER BY date DESC
            """
            
            # PHƯƠNG ÁN DỰ PHÒNG (Nếu date trong DB là DD/MM/YYYY):
            # Bạn cần chuyển đổi định dạng ngày trong câu truy vấn để so sánh chính xác:
            # query = f"""
            #    SELECT id, date, mcp, summary, download_url, details_clean , web_source 
            #    FROM {table_name} 
            #    WHERE SUBSTR(date, 7, 4) || '-' || SUBSTR(date, 4, 2) || '-' || SUBSTR(date, 1, 2) 
            #    BETWEEN ? AND ? 
            #    ORDER BY date DESC
            # """
            
            # Dùng start_date_str và end_date_str (ở định dạng YYYY-MM-DD)
            cursor.execute(query, (start_date_str, end_date_str))
            rows = cursor.fetchall()
            
            # Gán tag cho các hàng để hiển thị chi tiết sau này
            for row in rows:
                # Chỉ chèn 4 cột hiển thị (id,  mcp, date, summary) vào Treeview
                #tree_widget.insert('', 'end', values=row[0:5]) 
                
                # Lấy chi tiết sạch (cột thứ 6 - index 5)
                details_clean = row[6] 
               # download_url = row[4] # Lấy URL (cột thứ 5 - index 4)
                tree_widget.insert('', 'end', values=row[0:6], tags=(details_clean,))
                # ... (Logic gán tag giữ nguyên, sử dụng details_clean và download_url)

        messagebox.showinfo("Hoàn tất", f"Đã tải sự kiện mới từ {seven_days_ago} đến {today}.")

    except sqlite3.Error as e:
        messagebox.showerror("Lỗi Database", f"Lỗi khi tải sự kiện 7 ngày gần đây: {e}")
    finally:
        if conn:
            conn.close()


# --- LOGIC CHẠY SPIDER ---

def run_spider_subprocess(spider_name, output_file=None):
    """
    Thực thi lệnh scrapy crawl trong một tiến trình con (subprocess).
    """
    # Lấy đường dẫn tới file settings.py để đảm bảo Scrapy tìm thấy project
   # project_root = os.path.dirname(BASE_DIR) 
    project_root = BASE_DIR
    # Lệnh cơ sở (Sử dụng 'python -m scrapy' nếu 'scrapy' không nằm trong PATH)
    command = ['scrapy', 'crawl', spider_name]
    
    # Nếu có file output, thêm đối số -o
    if output_file:
        # Quan trọng: Đảm bảo file log 'new_events_today.txt' được xóa/ghi đè 
        # trong Pipeline của bạn trước mỗi lần chạy.
        command.extend(['-o', output_file, '-t', 'json']) 

    print(f"Bắt đầu chạy Scrapy: {' '.join(command)}")

    try:
        # Chạy lệnh. Giao diện người dùng sẽ không bị treo vì hàm này không block GUI.
        # cwd được thiết lập là thư mục gốc của dự án Scrapy
        process = subprocess.Popen(command, cwd=project_root, 
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, 
                                   text=True, shell=True)
        stdout, stderr = process.communicate()
        print(project_root)
        print(process.communicate())
        if process.returncode != 0:
            print(f"LỖI SCARPY TRONG TIẾN TRÌNH CON ({spider_name}):\n{stderr}")
            return f"❌ Lỗi chạy Scrapy ({spider_name}). Vui lòng kiểm tra terminal. Chi tiết: {stderr[:100]}..."
        else:
            print(f"Scrapy job ({spider_name}) hoàn thành thành công.")
            return f"✅ Hoàn thành Scrape: {spider_name}. Đã lưu vào DB và Log."

    except FileNotFoundError:
        return "❌ Lỗi: Lệnh 'scrapy' không được tìm thấy. Đảm bảo Scrapy đã cài đặt và nằm trong PATH."
    except Exception as e:
        return f"❌ Lỗi ngoại lệ khi chạy {spider_name}: {e}"


def handle_run_all_spiders(tree_widget, detail_box):
    """
    Quản lý việc chạy tất cả các spiders trong một luồng riêng biệt.
    """
    # Vô hiệu hóa nút trong khi đang chạy
    run_button.config(state=tk.DISABLED, text="⏳ Đang chạy Scrapy...")
    
    def run_all_async():
        results = []
        
        # SỬA: LẶP QUA DANH SÁCH ALL_SPIDERS VÀ CHẠY TỪNG SPIDER
        for spider_name in ALL_SPIDERS:
            # Gọi hàm run_spider_subprocess với tên spider hiện tại
            result = run_spider_subprocess(spider_name)
            results.append(result)

        # Cập nhật GUI sau khi chạy xong (cần sử dụng root.after)
        root.after(0, lambda: finalize_run(results, tree_widget, detail_box))
        
    # Bắt đầu luồng mới để không làm treo GUI
    threading.Thread(target=run_all_async).start()
    
def finalize_run(results, tree_widget, detail_box):
    """
    Cập nhật GUI và hiển thị kết quả sau khi Scrapy hoàn thành.
    """
    # Kích hoạt lại nút
    run_button.config(state=tk.NORMAL, text="🔄 Chạy Scrapers")
    
    # Hiển thị thông báo tổng hợp
    message = "\n".join(results)
    messagebox.showinfo("Kết quả Scrape", message)
    
    # Tải lại dữ liệu mới nhất (thường là Log file)
    display_new_events_7days(tree_widget, detail_box)


def load_history_from_selection(event, tree_widget, detail_box, combobox):
    """
    Xử lý sự kiện khi người dùng chọn một item từ Combobox.
    """
    selected_table = combobox.get()
    
    # Kiểm tra xem tên bảng có hợp lệ không
    if selected_table and selected_table in history_tables:
        display_history_data(tree_widget, selected_table, detail_box)
    else:
        # Xóa nội dung nếu người dùng chỉ nhập mà không chọn
        messagebox.showwarning("Lỗi Tên Bảng", f"'{selected_table}' không phải là tên bảng hợp lệ.")
        # Hoặc bạn có thể xóa nội dung combobox để làm sạch
        combobox.set('')

def filter_combobox_list(event, combobox):
    """
    Lọc danh sách các tùy chọn trong Combobox khi người dùng nhập liệu.
    """
    input_text = combobox.get().lower()
    
    if input_text == '':
        # Nếu không có gì được nhập, hiển thị tất cả
        combobox['values'] = history_tables
    else:
        # Lọc danh sách
        filtered_list = [table for table in history_tables if input_text in table.lower()]
        combobox['values'] = filtered_list

        # Nếu chỉ còn 1 item, chọn luôn item đó (tùy chọn)
        if len(filtered_list) == 1:
            combobox.set(filtered_list[0]) 
            
    # Giữ cửa sổ dropdown mở trong khi người dùng nhập
    combobox.event_generate('<Down>')

# --- 3. KHỞI TẠO GIAO DIỆN ---

# Khởi tạo cửa sổ chính
root = tk.Tk()
root.title("Stock Scraper GUI - Đang tải...")
root.geometry("1200x800")

# --- Tạo Khung Chính (Main Frame) ---
main_frame = ttk.Frame(root, padding="10 10 10 10")
main_frame.pack(fill='both', expand=True)

# --- Tạo Bảng Dữ liệu (Treeview) ---
columns = ('ID','Mã CP', 'Ngày Sự kiện', 'Tóm tắt', 'Scraped At', 'Nguồn')
tree = ttk.Treeview(main_frame, columns=columns, show='headings')

# Cài đặt Tiêu đề
tree.heading('ID', text='ID', anchor=tk.W)
tree.heading('Mã CP', text='Mã CP', anchor=tk.W)
tree.heading('Ngày Sự kiện', text='Ngày SK', anchor=tk.W)
tree.heading('Tóm tắt', text='Tóm tắt Sự kiện', anchor=tk.W)
tree.heading('Scraped At', text='Thời gian Scrape', anchor=tk.W)
tree.heading('Nguồn', text='Nguồn', anchor=tk.W)

# Cài đặt Chiều rộng Cột
tree.column('ID', width=50, anchor=tk.W)
tree.column('Mã CP', width=50, anchor=tk.W)
tree.column('Ngày Sự kiện', width=100, anchor=tk.W)
tree.column('Tóm tắt', width=450, anchor=tk.W)
tree.column('Scraped At', width=150, anchor=tk.W)
tree.column('Nguồn', width=80, anchor=tk.W)


tree.pack(side='top', fill='both', expand=False)

# Tạo Scrollbar cho Treeview
scrollbar = ttk.Scrollbar(main_frame, orient=tk.VERTICAL, command=tree.yview)
tree.configure(yscrollcommand=scrollbar.set)
scrollbar.pack(side='right', fill='y')

# --- Tạo Khu vực Chi tiết (Text Box) ---
ttk.Label(main_frame, text="Nội dung Chi tiết (details_clean):", font=('Arial', 10, 'bold')).pack(side='top', fill='x', pady=(10, 0))

detail_box = tk.Text(main_frame, height=15, state=tk.DISABLED, wrap=tk.WORD, bg='#f0f0f0', font=('Arial', 10), padx=5, pady=5)
detail_box.pack(side='bottom', fill='both', expand=True)

# Gắn sự kiện click vào Treeview để hiển thị chi tiết
tree.bind('<<TreeviewSelect>>', lambda event: on_item_select(event, tree, detail_box)) 

# --- Tạo Nút Điều hướng ---
btn_frame = ttk.Frame(main_frame)
btn_frame.pack(fill='x', pady=(10, 5))

# 1. Nút Xem Sự kiện Mới
# ttk.Button(btn_frame, 
#            text="🚨 SỰ KIỆN MỚI (Từ Log)", 
#            command=lambda: display_new_events(tree, detail_box)).pack(side='left', padx=5)

# Gắn sự kiện click vào Treeview để hiển thị chi tiết
tree.bind('<<TreeviewSelect>>', lambda event: on_item_select(event, tree, detail_box)) 

# --- Tạo Nút Điều hướng (Button Frame) ---
btn_frame = ttk.Frame(main_frame)
btn_frame.pack(fill='x', pady=(10, 5))

# ************ THÊM NÚT CHẠY SPIDERS ************
run_button = ttk.Button(btn_frame, 
                        text="🔄 Chạy Scrapers", 
                        command=lambda: handle_run_all_spiders(tree, detail_box))
run_button.pack(side='left', padx=10)
# **********************************************

# 1. Nút Xem Sự kiện Mới
ttk.Button(btn_frame, 
           text="🚨 SỰ KIỆN MỚI (7 ngày)", 
           command=lambda: display_new_events_7days(tree, detail_box)).pack(side='left', padx=5)

# 2. Các nút Xem Lịch sử (Tên các bảng trong SQLite)
history_tables = ALL_SPIDERS

# ************ THAY THẾ CÁC NÚT LỊCH SỬ BẰNG COMBOBOX TÌM KIẾM ************
ttk.Label(btn_frame, text=" | Chọn Bảng Lịch sử:").pack(side='left', padx=(20, 5))

# Tạo Combobox
history_combo = ttk.Combobox(btn_frame, values=history_tables, state='normal', width=25)
history_combo.pack(side='left', padx=5)

# Gắn sự kiện để lọc khi người dùng gõ
history_combo.bind('<KeyRelease>', lambda event: filter_combobox_list(event, history_combo))

# Gắn sự kiện để tải dữ liệu khi người dùng chọn (Enter hoặc click)
history_combo.bind('<<ComboboxSelected>>', lambda event: load_history_from_selection(event, tree, detail_box, history_combo))

# --- Chạy ứng dụng ---
# Tải dữ liệu mặc định là Sự kiện Mới khi khởi động
display_new_events_7days(tree, detail_box)

root.mainloop()