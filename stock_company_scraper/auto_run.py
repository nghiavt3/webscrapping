import schedule
import time
import subprocess
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# Thử import danh sách spider
try:
    from spider_names import ALL_SPIDERS
except ImportError:
    ALL_SPIDERS = []

# --- CẤU HÌNH ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MAX_WORKERS = 5  # Số luồng chạy song song khi chạy tự động
INTERVAL_MINUTES = 10 # Cứ mỗi 10 phút quét 1 lần

def run_single_spider(spider_name):
    """Thực thi lệnh scrapy crawl cho từng spider."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Đang quét: {spider_name}...")
    try:
        # Sử dụng subprocess.run để đợi spider hoàn thành
        subprocess.run(['scrapy', 'crawl', spider_name], 
                       cwd=BASE_DIR, 
                       shell=True, 
                       capture_output=True)
        return f"Xong {spider_name}"
    except Exception as e:
        return f"Lỗi {spider_name}: {e}"

def job():
    """Công việc chính: Chạy toàn bộ spider song song."""
    print(f"\n{'='*20}")
    print(f"BẮT ĐẦU CHU KỲ QUÉT TỰ ĐỘNG: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*20}")

    if not ALL_SPIDERS:
        print("Cảnh báo: Danh sách ALL_SPIDERS trống!")
        return

    # Chạy song song để tiết kiệm thời gian
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(run_single_spider, ALL_SPIDERS))
    
    for res in results:
        print(res)

    print(f"\nChu kỳ hoàn tất. Đợi {INTERVAL_MINUTES} phút cho lần quét tiếp theo...")

# --- KHỞI CHẠY ---
if __name__ == "__main__":
    # 1. Chạy lần đầu tiên ngay khi mở file
    job()

    # 2. Lập lịch định kỳ
    schedule.every(INTERVAL_MINUTES).minutes.do(job)

    print(f"Hệ thống Auto-Run đã sẵn sàng. Tần suất: {INTERVAL_MINUTES} phút/lần.")
    
    while True:
        schedule.run_pending()
        time.sleep(1)