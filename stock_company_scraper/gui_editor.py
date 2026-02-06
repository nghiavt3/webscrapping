import sqlite3
import tkinter as tk
from tkinter import ttk, messagebox
import os
import re

# --- CẤU HÌNH ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_NAME = os.path.join(BASE_DIR, 'stock_events.db')

class DataEditorApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Trình Chỉnh Sửa & Fix ID LuatVietNam")
        self.root.geometry("1200x700")
        
        self.current_table = tk.StringVar()
        
        # --- 1. Khu vực điều khiển (Top Bar) ---
        top_frame = ttk.Frame(root, padding="10")
        top_frame.pack(fill='x')
        
        ttk.Label(top_frame, text="Chọn bảng:").pack(side='left', padx=5)
        self.table_combo = ttk.Combobox(top_frame, textvariable=self.current_table, state='readonly', width=25)
        self.table_combo.pack(side='left', padx=5)
        self.table_combo.bind("<<ComboboxSelected>>", self.load_data)
        
        ttk.Button(top_frame, text="🔄 Refresh DS Bảng", command=self.refresh_table_list).pack(side='left', padx=5)
        
        # Nút chức năng đặc biệt cho LuatVietNam
        fix_btn = ttk.Button(top_frame, text="🛠 Fix ID LuatVietNam", command=self.confirm_fix_ids)
        fix_btn.pack(side='left', padx=20)
        
        ttk.Label(top_frame, text="(Double-click ô để SỬA | Del để XÓA)", foreground="#555").pack(side='right')

        # --- 2. Bảng hiển thị dữ liệu (Treeview) ---
        self.tree_frame = ttk.Frame(root, padding="10")
        self.tree_frame.pack(fill='both', expand=True)
        
        # Tạo thanh cuộn
        self.scrollbar_y = ttk.Scrollbar(self.tree_frame, orient="vertical")
        self.scrollbar_x = ttk.Scrollbar(self.tree_frame, orient="horizontal")
        
        self.tree = ttk.Treeview(self.tree_frame, show='headings', 
                                 yscrollcommand=self.scrollbar_y.set, 
                                 xscrollcommand=self.scrollbar_x.set)
        
        self.scrollbar_y.config(command=self.tree.yview)
        self.scrollbar_x.config(command=self.tree.xview)
        
        self.tree.pack(side='top', fill='both', expand=True)
        self.scrollbar_y.pack(side='right', fill='y')
        self.scrollbar_x.pack(side='bottom', fill='x')

        # Gán sự kiện
        self.tree.bind("<Double-1>", self.on_double_click)
        self.tree.bind("<Delete>", self.delete_record)

        # Khởi tạo danh sách bảng
        self.refresh_table_list()

    def refresh_table_list(self):
        """Lấy danh sách các bảng event_ từ database"""
        if not os.path.exists(DATABASE_NAME):
            messagebox.showerror("Lỗi", "Không tìm thấy file stock_events.db")
            return
        
        try:
            conn = sqlite3.connect(DATABASE_NAME)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'event_%'")
            tables = [row[0] for row in cursor.fetchall()]
            conn.close()
            
            self.table_combo['values'] = sorted(tables)
            if tables:
                if "event_luatvietnam" in tables:
                    self.table_combo.set("event_luatvietnam")
                else:
                    self.table_combo.current(0)
                self.load_data()
        except Exception as e:
            messagebox.showerror("Lỗi DB", str(e))

    def load_data(self, event=None):
        """Tải dữ liệu của bảng được chọn"""
        table = self.current_table.get()
        if not table: return

        # Xóa dữ liệu cũ
        for item in self.tree.get_children():
            self.tree.delete(item)

        try:
            conn = sqlite3.connect(DATABASE_NAME)
            cursor = conn.cursor()
            
            # Lấy cấu trúc cột
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [info[1] for info in cursor.fetchall()]
            self.tree["columns"] = columns
            
            for col in columns:
                self.tree.heading(col, text=col)
                self.tree.column(col, width=150, minwidth=100)
            
            # Tóm tắt thường dài nên cho rộng ra
            if 'summary' in columns:
                self.tree.column('summary', width=400)

            # Lấy dữ liệu (giới hạn 500 bản ghi mới nhất để tránh lag)
            cursor.execute(f"SELECT * FROM {table} ORDER BY rowid DESC LIMIT 500")
            for row in cursor.fetchall():
                self.tree.insert("", "end", values=row)
            
            conn.close()
        except Exception as e:
            messagebox.showerror("Lỗi tải dữ liệu", str(e))

    def on_double_click(self, event):
        """Mở cửa sổ sửa khi nhấn đúp vào ô"""
        item_id = self.tree.focus()
        if not item_id: return
        
        column = self.tree.identify_column(event.x)
        col_idx = int(column[1:]) - 1
        col_name = self.tree["columns"][col_idx]
        
        current_values = self.tree.item(item_id, 'values')
        old_value = current_values[col_idx]
        row_primary_id = current_values[0] # Giả định ID ở cột 0

        # Tạo popup sửa
        win = tk.Toplevel(self.root)
        win.title(f"Sửa {col_name}")
        win.geometry("500x200")
        
        ttk.Label(win, text=f"Chỉnh sửa nội dung cho cột [{col_name}]:", font=('Arial', 10, 'bold')).pack(pady=10)
        txt_area = tk.Text(win, height=4, width=50)
        txt_area.insert("1.0", old_value)
        txt_area.pack(padx=10, pady=5)
        
        def save_edit():
            new_val = txt_area.get("1.0", "end-1c").strip()
            table = self.current_table.get()
            primary_key = self.tree["columns"][0]
            
            try:
                conn = sqlite3.connect(DATABASE_NAME)
                cursor = conn.cursor()
                query = f"UPDATE {table} SET {col_name} = ? WHERE {primary_key} = ?"
                cursor.execute(query, (new_val, row_primary_id))
                conn.commit()
                conn.close()
                
                # Cập nhật UI
                new_vals = list(current_values)
                new_vals[col_idx] = new_val
                self.tree.item(item_id, values=new_vals)
                win.destroy()
            except Exception as e:
                messagebox.showerror("Lỗi Update", str(e))

        ttk.Button(win, text="💾 LƯU THAY ĐỔI", command=save_edit).pack(pady=10)

    def delete_record(self, event):
        """Xóa dòng dữ liệu"""
        selected = self.tree.focus()
        if not selected: return
        
        if messagebox.askyesno("Xác nhận", "Bạn muốn xóa bản ghi này khỏi Database?"):
            item_vals = self.tree.item(selected, 'values')
            row_id = item_vals[0]
            table = self.current_table.get()
            pk = self.tree["columns"][0]

            try:
                conn = sqlite3.connect(DATABASE_NAME)
                cursor = conn.cursor()
                cursor.execute(f"DELETE FROM {table} WHERE {pk} = ?", (row_id,))
                conn.commit()
                conn.close()
                self.tree.delete(selected)
            except Exception as e:
                messagebox.showerror("Lỗi xóa", str(e))

    def confirm_fix_ids(self):
        """Hàm logic để fix ID cho LuatVietNam"""
        table = self.current_table.get()
        if table != "event_luatvietnam":
            messagebox.showwarning("Chú ý", "Chức năng này chỉ dành riêng cho bảng 'event_luatvietnam'")
            return
            
        if messagebox.askyesnocancel("Xác nhận", "Hệ thống sẽ tự động cập nhật lại toàn bộ ID dựa trên Summary và Date.\nBạn có muốn tiếp tục?"):
            self.run_fix_ids_logic(table)

    def run_fix_ids_logic(self, table):
        try:
            conn = sqlite3.connect(DATABASE_NAME)
            cursor = conn.cursor()
            
            # Lấy ID cũ, Date và Summary để tính toán
            cursor.execute(f"SELECT rowid, date, summary FROM {table}")
            rows = cursor.fetchall()
            
            count = 0
            for row_id, doc_date, summary in rows:
                if not summary or not doc_date: continue
                
                # Regex trích xuất phần đầu đến chữ "của" hoặc "do"
                match = re.search(r'^(.*?)\s+(của|do)\b', summary, re.IGNORECASE)
                prefix = match.group(1).strip() if match else summary[:30].strip()
                
                # Làm sạch prefix: bỏ ký tự đặc biệt, thay khoảng trắng bằng gạch dưới
                clean_prefix = re.sub(r'[^\w\s\-/]', '', prefix)
                new_id = f"{clean_prefix}_{doc_date}".replace(" ", "_")
                
                # Cập nhật ID mới
                cursor.execute(f"UPDATE {table} SET id = ? WHERE rowid = ?", (new_id, row_id))
                count += 1
                
            conn.commit()
            conn.close()
            messagebox.showinfo("Thành công", f"Đã cập nhật xong {count} bản ghi.")
            self.load_data() # Reload bảng
        except Exception as e:
            messagebox.showerror("Lỗi khi Fix ID", str(e))

if __name__ == "__main__":
    root = tk.Tk()
    app = DataEditorApp(root)
    root.mainloop()