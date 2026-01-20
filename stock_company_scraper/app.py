import os
import subprocess
import json
from flask import Flask, request, jsonify, redirect, url_for, session, render_template_string

# Import thư viện Firebase Admin
import firebase_admin
from firebase_admin import credentials, auth

import hashlib
import time
# --- 1. CẤU HÌNH BẢO MẬT & FIREBASE ---

SECRET_KEY = 'b3e8c7f3a4d9e0b2a1c6f5e4d3c2b1a0e9f8d7c6b5a4e3d2f1e0d9c8b7a6f5e4'
SERVICE_ACCOUNT_FILE = 'firebase-admin-key.json' # Thay thế bằng tên file của bạn

# DANH SÁCH EMAIL ADMIN
# Chỉ những email này mới được cấp quyền 'admin' để chạy Scrapy
ADMIN_EMAILS = [
    "vuongtrongnghia91@gmail.com", 
    "thaovps@gmail.com"
] 

# Khởi tạo Firebase Admin SDK
try:
    cred = credentials.Certificate(SERVICE_ACCOUNT_FILE)
    firebase_admin.initialize_app(cred)
except FileNotFoundError:
    print("LỖI: Không tìm thấy file Service Account Key. Vui lòng tải file JSON từ Firebase Console.")
    exit(1)
except ValueError:
    # Tránh lỗi nếu app đã được initialize
    pass 

# --- 2. KHỞI TẠO FLASK VÀ LOGIC KIỂM TRA QUYỀN ---

app = Flask(__name__)
app.config['SECRET_KEY'] = SECRET_KEY

# Hàm kiểm tra quyền
def is_admin():
    """Kiểm tra xem người dùng hiện tại có phải là Admin không."""
    return session.get('role') == 'admin'

# Hàm kích hoạt Scrapy (Giữ nguyên)
def run_scrapy_spider(spider_name):
    """Hàm thực thi tiến trình Scrapy."""
    print(f"--- Đang cố gắng chạy spider: {spider_name} ---")
    try:
        command = ['scrapy', 'crawl', spider_name]
        subprocess.Popen(command) 
        return True
    except FileNotFoundError:
        print("Lỗi: Không tìm thấy lệnh 'scrapy' hoặc dự án Scrapy.")
        return False
    except Exception as e:
        print(f"Lỗi không xác định khi chạy Scrapy: {e}")
        return False

# Thay thế hàm run_scrapy_spider bằng hàm mới:
def start_external_script(script_name):
    print(f"--- Đang khởi chạy file: {script_name} ---")
    try:
        # Tạo token dựa trên thời gian (thay đổi sau mỗi phút)
        # Chuỗi gốc: "BiMat2025" + "2025-12-30 14:30"
        time_str = time.strftime('%Y-%m-%d %H:%M') 
        raw_string = f"MySecretKey_{time_str}"
        dynamic_token = hashlib.sha256(raw_string.encode()).hexdigest()

        # Truyền token động vào tham số
        command = ['python', script_name, dynamic_token]
        subprocess.Popen(command) 
        return True
    except Exception as e:
        print(f"Lỗi: {e}")
        return False
# --- 3. ENDPOINT XỬ LÝ ĐĂNG NHẬP/ĐĂNG KÝ ---

@app.route('/')
def index():
    user_email = session.get('user_email')
    
    if user_email:
        role = session.get('role', 'guest')
        
        # Giao diện khi đã đăng nhập
        if role == 'admin':
            scrapy_link = f'<p><a href="{url_for("run_scrapy")}">BẤM ĐỂ CHẠY SCRAPY (ADMIN)</a></p>'
        else:
            scrapy_link = '<p>Bạn không có quyền Admin để chạy Scrapy.</p>'
            
        return render_template_string(f"""
            <h1>Xin chào, {user_email}</h1>
            <p>Vai trò của bạn: <b>{role.upper()}</b></p>
            {scrapy_link}
            <p><a href="{url_for('logout')}">Đăng xuất</a></p>
        """)
    
    # Giao diện khi chưa đăng nhập
    return render_template_string(f"""
        <h1>Ứng dụng Scrapy Bảo Mật</h1>
        <form method="POST" action="{url_for('login_api')}">
            <input type="email" name="email" placeholder="Email" required><br><br>
            <input type="password" name="password" placeholder="Password" required><br><br>
            <button type="submit">Đăng nhập</button>
        </form>
        <p>Lưu ý: Ứng dụng này yêu cầu người dùng được tạo qua Firebase Console hoặc API</p>
    """)


@app.route('/login', methods=['POST'])
def login_api():
    """
    Xử lý việc xác minh token đăng nhập.
    Lưu ý: Việc đăng nhập trực tiếp (Email/Pass) từ Server (Admin SDK) không được khuyến khích
    vì nó không tạo ra Session Token như Client SDK.
    CÁCH TỐT NHẤT LÀ: Client Web/App dùng Client SDK để đăng nhập -> Lấy ID Token -> Gửi ID Token lên Flask.
    """
    
    # *** GIẢ ĐỊNH DÙNG CÁCH DỄ NHẤT: TRỰC TIẾP TỪ FORM VÀ XÁC THỰC BẰNG Admin SDK ***
    
    email = request.form.get('email')
    password = request.form.get('password')

    if not email or not password:
        return "Thiếu Email hoặc Password", 400

    try:
        # 1. Tìm người dùng dựa trên email
        user = auth.get_user_by_email(email)
        
        # NOTE: Admin SDK KHÔNG THỂ XÁC MINH PASSWORD! 
        # Để xác minh password, bạn CẦN SỬ DỤNG REST API CỦA FIREBASE HOẶC CLIENT SDK.
        # Ở đây, chúng ta chỉ xác minh tài khoản tồn tại và tin cậy vào việc user tự nhập đúng pass.
        # ĐỂ ĐẢM BẢO BẢO MẬT, BẠN CẦN THAM KHẢO PHƯƠNG PHÁP XÁC THỰC ID TOKEN (DÙNG JS SDK TRÊN CLIENT).
        
        # 2. Tạo Session và Kiểm tra Role
        session['user_email'] = user.email
        
        if user.email in ADMIN_EMAILS:
            session['role'] = 'admin'
        else:
            session['role'] = 'guest'
            
        return redirect(url_for('index'))
        
    except auth.UserNotFoundError:
        return "Người dùng không tồn tại.", 401
    except Exception as e:
        print(f"Lỗi Firebase: {e}")
        return f"Lỗi xác thực: {e}", 500

@app.route('/logout')
def logout():
    """Xóa session và đăng xuất."""
    session.clear()
    return redirect(url_for('index'))


@app.route('/run_scrapy')
def run_scrapy():
    """ENDPOINT BẢO VỆ - CHỈ DÀNH CHO ADMIN"""
    
    if not session.get('user_email'):
        return redirect(url_for('index')) 
    
    if not is_admin():
        return "<h1>Truy cập Bị Từ Chối</h1><p>Bạn không phải là Admin.</p>", 403
    
    # Nếu là Admin, tiến hành chạy Scrapy
    # --- PHẦN THAY ĐỔI CẦN THIẾT ---
    script_to_run = "gui_tracker.py" # <--- Tên file bạn muốn chạy
    # <p>Đã gửi lệnh khởi chạy file <b>{0}</b>. Vui lòng kiểm tra màn hình desktop.</p>
    if start_external_script(script_to_run):
        return render_template_string("""
            <h1>Thành Công!</h1>
            <p>Đã gửi lệnh khởi chạy. Vui lòng kiểm tra màn hình desktop.</p>
            <p><a href="{1}">Quay lại trang chủ</a></p>
        """.format(script_to_run, url_for('index')))
    # ----------------------------------
    else:
        return "<h1>Lỗi!</h1><p>Không thể kích hoạt Scrapy Spider.</p>", 500

if __name__ == '__main__':
    app.run(debug=True)