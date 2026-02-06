import sqlite3
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import os

# Cấu hình đường dẫn Database
DATABASE_NAME = 'stock_events.db'

class AICacheManager:
    def __init__(self, root):
        self.root = root
        self.root.title("AI Cache Manager - Công cụ quản lý điểm số AI")
        self.root.geometry("1100x700")
        self.root.configure(bg="#f8f9fa")

        # --- GIAO DIỆN TÌM KIẾM ---
        search_frame = tk.Frame(self.root, bg="#f8f9fa", pady=15)
        search_frame.pack(fill='x')
        
        tk.Label(search_frame, text="🔍 Tìm kiếm (Mã CP/URL):", bg="#f8f9fa", font=('Segoe UI', 10, 'bold')).pack(side='left', padx=15)
        self.search_var = tk.StringVar()
        self.search_entry = ttk.Entry(search_frame, textvariable=self.search_var, width=50)
        self.search_entry.pack(side='left', padx=5)
        self.search_entry.bind('<KeyRelease>', lambda e: self.load_data())

        # --- BẢNG DỮ LIỆU ---
        table_frame = tk.Frame(self.root)
        table_frame.pack(fill='both', expand=True, padx=15, pady=5)

        columns = ('url', 'mcp', 'score', 'time')
        self.tree = ttk.Treeview(table_frame, columns=columns, show='headings', height=20)
        
        self.tree.heading('url', text='Đường dẫn PDF (URL)')
        self.tree.heading('mcp', text='Mã CP')
        self.tree.heading('score', text='Điểm AI (-10 đến +10)')
        self.tree.heading('time', text='Ngày tạo')
        
        self.tree.column('url', width=500)
        self.tree.column('mcp', width=100, anchor='center')
        self.tree.column('score', width=150, anchor='center')
        self.tree.column('time', width=180, anchor='center')

        # Thanh cuộn
        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        self.tree.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')

        # Tags màu sắc cho hàng
        self.tree.tag_configure('positive', background='#e6ffed') # Tin tốt - Xanh lá
        self.tree.tag_configure('negative', background='#fff1f0') # Tin xấu - Đỏ nhạt
        self.tree.tag_configure('missing', background='#fff9c4')  # Thiếu mã CP - Vàng nhạt

        # --- THANH CÔNG CỤ (BUTTONS) ---
        btn_frame = tk.Frame(self.root, bg="#f8f9fa", pady=15)
        btn_frame.pack(fill='x')

        ttk.Button(btn_frame, text="🔄 Làm mới", command=self.load_data).pack(side='left', padx=15)
        
        btn_edit_score = ttk.Button(btn_frame, text="⭐ Sửa Điểm Score", command=self.edit_score_manual)
        btn_edit_score.pack(side='left', padx=5)

        btn_edit_mcp = ttk.Button(btn_frame, text="✏️ Sửa Mã CP", command=self.on_double_click)
        btn_edit_mcp.pack(side='left', padx=5)
        
        ttk.Button(btn_frame, text="🗑️ Xóa dòng này", command=self.delete_entry).pack(side='right', padx=15)

        # Hướng dẫn nhanh
        lbl_hint = tk.Label(self.root, text="💡 Mẹo: Double-click để sửa nhanh Mã CP | Chọn dòng rồi bấm 'Sửa Điểm Score' để cập nhật điểm số.", 
                           bg="#f8f9fa", fg="#666", font=("Segoe UI", 9, "italic"))
        lbl_hint.pack(pady=5)

        # Sự kiện click chuột
        self.tree.bind("<Double-1>", lambda e: self.on_double_click())

        # Load dữ liệu ban đầu
        if os.path.exists(DATABASE_NAME):
            self.load_data()
        else:
            messagebox.showerror("Lỗi", f"Không tìm thấy file cơ sở dữ liệu: {DATABASE_NAME}")

    def load_data(self):
        """Tải dữ liệu từ database vào bảng"""
        for item in self.tree.get_children():
            self.tree.delete(item)
            
        search_query = self.search_var.get().strip().upper()
        
        try:
            conn = sqlite3.connect(DATABASE_NAME)
            cursor = conn.cursor()
            
            query = "SELECT pdf_url, mcp, sentiment_score, created_at FROM ai_cache"
            params = []
            
            if search_query:
                query += " WHERE mcp LIKE ? OR pdf_url LIKE ?"
                params = [f'%{search_query}%', f'%{search_query}%']
            
            query += " ORDER BY created_at DESC"
            cursor.execute(query, params)
            
            for row in cursor.fetchall():
                url, mcp, score, created_at = row
                
                # CHỐNG LỖI None: Xử lý hiển thị score
                display_score = score if score is not None else 0
                display_mcp = mcp if mcp else ""
                
                # Xác định tag màu sắc
                tag = ''
                if not display_mcp:
                    tag = 'missing'
                elif int(display_score) > 0:
                    tag = 'positive'
                elif int(display_score) < 0:
                    tag = 'negative'
                
                self.tree.insert('', 'end', values=(url, display_mcp, display_score, created_at), tags=(tag,))
                
            conn.close()
        except Exception as e:
            print(f"Lỗi database: {e}")

    def edit_score_manual(self):
        """Hàm cập nhật điểm số AI - Đã xử lý lỗi ValueError int(None)"""
        selected = self.tree.focus()
        if not selected:
            messagebox.showwarning("Chú ý", "Vui lòng chọn một dòng trên bảng để sửa điểm!")
            return
        
        item_data = self.tree.item(selected, 'values')
        url = item_data[0]
        raw_score = item_data[2]
        
        # XỬ LÝ LỖI: Kiểm tra an toàn trước khi ép kiểu int
        try:
            if raw_score is None or str(raw_score).lower() == 'none' or raw_score == '':
                current_score = 0
            else:
                current_score = int(raw_score)
        except (ValueError, TypeError):
            current_score = 0
        
        # Hiện hộp thoại nhập số
        new_score = simpledialog.askinteger("Cập nhật Score", 
                                            f"Cập nhật điểm AI cho:\n{url[:60]}...\n\nNhập điểm (-10 đến 10):", 
                                            initialvalue=current_score,
                                            minvalue=-10, maxvalue=10)
        
        if new_score is not None:
            self.update_db(url, score=new_score)

    def on_double_click(self, event=None):
        """Sửa Mã CP khi double click hoặc bấm nút"""
        selected = self.tree.focus()
        if not selected: return
        
        item_data = self.tree.item(selected, 'values')
        url = item_data[0]
        current_mcp = item_data[1]
        
        new_mcp = simpledialog.askstring("Cập nhật Mã CP", 
                                         f"Nhập mã chứng khoán mới cho URL này:", 
                                         initialvalue=current_mcp)
        
        if new_mcp is not None:
            self.update_db(url, mcp=new_mcp.strip().upper())

    def update_db(self, url, mcp=None, score=None):
        """Lưu thay đổi vào SQLite"""
        try:
            conn = sqlite3.connect(DATABASE_NAME)
            cursor = conn.cursor()
            
            if mcp is not None:
                cursor.execute("UPDATE ai_cache SET mcp = ? WHERE pdf_url = ?", (mcp, url))
            
            if score is not None:
                cursor.execute("UPDATE ai_cache SET sentiment_score = ? WHERE pdf_url = ?", (score, url))
                
            conn.commit()
            conn.close()
            self.load_data() # Refresh lại giao diện
        except Exception as e:
            messagebox.showerror("Lỗi", f"Không thể cập nhật cơ sở dữ liệu: {e}")

    def delete_entry(self):
        """Xóa hoàn toàn một bản ghi cache"""
        selected = self.tree.focus()
        if not selected: return
        
        url = self.tree.item(selected, 'values')[0]
        confirm = messagebox.askyesno("Xác nhận xóa", f"Bạn có chắc muốn xóa cache cho URL này?\n(AI sẽ phải phân tích lại từ đầu nếu bạn mở lại tin này)")
        
        if confirm:
            try:
                conn = sqlite3.connect(DATABASE_NAME)
                cursor = conn.cursor()
                cursor.execute("DELETE FROM ai_cache WHERE pdf_url = ?", (url,))
                conn.commit()
                conn.close()
                self.load_data()
            except Exception as e:
                messagebox.showerror("Lỗi", f"Lỗi khi xóa: {e}")

if __name__ == "__main__":
    root = tk.Tk()
    # Cấu hình icon hoặc style nếu muốn
    style = ttk.Style()
    style.theme_use('clam') # Giao diện hiện đại hơn một chút
    
    app = AICacheManager(root)
    root.mainloop()