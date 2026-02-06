import os
import subprocess
import hashlib
import time
import requests  # Cần cài đặt: pip install requests
from flask import Flask, request, jsonify, redirect, url_for, session, render_template_string

import firebase_admin
from firebase_admin import credentials, auth

# --- 1. CẤU HÌNH ---
SECRET_KEY = 'b3e8c7f3a4d9e0b2a1c6f5e4d3c2b1a0e9f8d7c6b5a4e3d2f1e0d9c8b7a6f5e4'
SERVICE_ACCOUNT_FILE = 'firebase-admin-key.json'
# LẤY API KEY NÀY TỪ FIREBASE CONSOLE (Project Settings -> General)
FIREBASE_WEB_API_KEY = "AIzaSyB3egPoxC73_QnazLiOCnJiyASMNb5gPqw" 

ADMIN_EMAILS = ["vuongtrongnghia91@gmail.com", "thaovps@gmail.com"]

try:
    cred = credentials.Certificate(SERVICE_ACCOUNT_FILE)
    firebase_admin.initialize_app(cred)
except Exception as e:
    print(f"Lỗi Firebase Admin: {e}")

app = Flask(__name__)
app.config['SECRET_KEY'] = SECRET_KEY

# --- 2. LOGIC HỖ TRỢ ---

def set_user_session(email):
    """Thiết lập session chung cho cả 2 phương thức."""
    session['user_email'] = email
    session['role'] = 'admin' if email in ADMIN_EMAILS else 'guest'

def start_external_script(script_name):
    try:
        time_str = time.strftime('%Y-%m-%d %H:%M') 
        raw_string = f"MySecretKey_{time_str}"
        dynamic_token = hashlib.sha256(raw_string.encode()).hexdigest()
        subprocess.Popen(['python', script_name, dynamic_token]) 
        return True
    except Exception:
        return False

# --- 3. GIAO DIỆN HYBRID (HTML + JS) ---

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Hệ thống Scrapy</title>
    <meta charset="utf-8">
    <script src="https://www.gstatic.com/firebasejs/10.7.1/firebase-app-compat.js"></script>
    <script src="https://www.gstatic.com/firebasejs/10.7.1/firebase-auth-compat.js"></script>
</head>
<body style="font-family: Arial; text-align: center; padding-top: 50px;">

    {% if not user_email %}
        <div style="max-width: 300px; margin: auto; border: 1px solid #ddd; padding: 20px; border-radius: 8px;">
            <h2>Đăng nhập</h2>
            
            <form method="POST" action="{{ url_for('login_email') }}">
                <input type="email" name="email" placeholder="Email" required style="width: 90%; margin-bottom: 10px; padding: 8px;"><br>
                <input type="password" name="password" placeholder="Mật khẩu" required style="width: 90%; margin-bottom: 10px; padding: 8px;"><br>
                <button type="submit" style="width: 96%; padding: 10px; background: #007bff; color: white; border: none; cursor: pointer;">Đăng nhập Email</button>
            </form>

            <div style="margin: 20px 0; border-top: 1px solid #eee; padding-top: 20px;">
                <p>Hoặc</p>
                <button id="google-login" style="width: 96%; padding: 10px; background: white; border: 1px solid #444; cursor: pointer; display: flex; align-items: center; justify-content: center;">
                    <img src="https://www.gstatic.com/firebasejs/ui/2.0.0/images/auth/google.svg" width="18" style="margin-right: 10px;">
                    Tiếp tục với Google
                </button>
            </div>
        </div>
    {% else %}
        <h1>Chào mừng, {{ user_email }}</h1>
        <p>Quyền: <b style="color: blue;">{{ role.upper() }}</b></p>
        {% if role == 'admin' %}
            <a href="{{ url_for('run_scrapy') }}" style="display: inline-block; background: red; color: white; padding: 15px 25px; text-decoration: none; font-weight: bold; border-radius: 5px;">CHẠY SCRAPY</a>
        {% endif %}
        <p><a href="{{ url_for('logout') }}">Thoát</a></p>
    {% endif %}

    <script>
        const firebaseConfig = {
        apiKey: "AIzaSyB3egPoxC73_QnazLiOCnJiyASMNb5gPqw",
        authDomain: "webscrapping-d300c.firebaseapp.com",
        projectId: "webscrapping-d300c",
        storageBucket: "webscrapping-d300c.firebasestorage.app",
        messagingSenderId: "787695285726",
        appId: "1:787695285726:web:4e996fdf3f41b53598d94f",
        measurementId: "G-QNRL2CS2EQ"
        };
        firebase.initializeApp(firebaseConfig);

        const googleBtn = document.getElementById('google-login');
        if (googleBtn) {
            googleBtn.onclick = () => {
                const provider = new firebase.auth.GoogleAuthProvider();
                firebase.auth().signInWithPopup(provider)
                .then(result => result.user.getIdToken())
                .then(idToken => {
                    return fetch('/verify-google', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ idToken: idToken })
                    });
                })
                .then(res => { if (res.ok) window.location.reload(); });
            };
        }
    </script>
</body>
</html>
"""

# --- 4. CÁC ENDPOINT ---

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE, 
                                 user_email=session.get('user_email'), 
                                 role=session.get('role'),
                                 api_key=FIREBASE_WEB_API_KEY)

@app.route('/login-email', methods=['POST'])
def login_email():
    """Xác thực Email/Password bằng Firebase Auth REST API."""
    email = request.form.get('email')
    password = request.form.get('password')
    
    # URL xác thực của Firebase
    url = f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={FIREBASE_WEB_API_KEY}"
    payload = {"email": email, "password": password, "returnSecureToken": True}
    
    res = requests.post(url, json=payload)
    if res.status_code == 200:
        set_user_session(email)
        return redirect(url_for('index'))
    else:
        return f"Lỗi đăng nhập: {res.json().get('error', {}).get('message', 'Unknown error')}", 401

@app.route('/verify-google', methods=['POST'])
def verify_google():
    """Xác thực Token Google gửi từ JS."""
    id_token = request.json.get('idToken')
    try:
        decoded_token = auth.verify_id_token(id_token)
        set_user_session(decoded_token['email'])
        return jsonify({'status': 'ok'})
    except:
        return jsonify({'status': 'error'}), 401

@app.route('/run_scrapy')
def run_scrapy():
    if session.get('role') != 'admin':
        return "Từ chối truy cập", 403
    
    if start_external_script("gui_tracker.py"):
        return "<h1>Đã khởi chạy!</h1><a href='/'>Quay lại</a>"
    return "Lỗi thực thi", 500

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True)