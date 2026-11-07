"""Sinh dữ liệu mẫu cho các bài thực hành học phần Dữ liệu lớn.

Cách dùng (chạy trong thư mục moi-truong-bigdata-lab/, cần Python >= 3.8 trên máy host):
    python3 gen_data.py --rows 5000000 --out ./data          # ~500 MB CSV giao dịch
    python3 gen_data.py --rows 200000  --out ./data --small   # bản rút gọn cho máy yếu

Hoặc chạy bằng chính image của cụm (không cần cài Python trên host):
    docker run --rm -v "$PWD":/w -w /w bigdata-lab:2026.10 python3 gen_data.py --rows 5000000 --out ./data

Thư mục ./data trên host được mount thành /data trong namenode, spark-master, nodemanager1/2,
hiveserver2, hbase (và /import trong mongodb), nên không cần "docker cp".

Sinh ra:
    retail_transactions.csv  : giao dịch bán lẻ (10 cột, có sẵn lệch dữ liệu theo khu vực)
    products.csv             : bảng danh mục sản phẩm nhỏ, dùng cho bài join
    web_access.log           : log truy cập web định dạng Apache combined
    sensor_readings.csv      : dữ liệu cảm biến IoT, dùng cho bài thiết kế row key HBase
"""
import argparse, random, datetime as dt, os

CATEGORIES = ["dien_tu", "thoi_trang", "gia_dung", "thuc_pham", "sach", "the_thao", "me_va_be"]
# Cố ý để HCM và Hanoi chiếm phần lớn giao dịch nhằm tạo lệch dữ liệu (data skew) cho bài tối ưu
REGIONS = ["HCM"] * 45 + ["Hanoi"] * 30 + ["DaNang"] * 8 + ["CanTho"] * 6 + \
          ["HaiPhong"] * 5 + ["NgheAn"] * 3 + ["LamDong"] * 3
URLS = ["/", "/san-pham", "/san-pham/dien-tu", "/gio-hang", "/thanh-toan", "/tim-kiem",
        "/dang-nhap", "/api/v1/products", "/api/v1/orders", "/khuyen-mai"]
STATUS = [200] * 80 + [301] * 5 + [404] * 8 + [500] * 4 + [503] * 3
AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0)",
          "Mozilla/5.0 (X11; Linux x86_64)", "curl/8.4.0"]


def gen_products(path, n=500):
    with open(os.path.join(path, "products.csv"), "w", encoding="utf-8") as f:
        f.write("product_id,product_name,category,brand,base_price\n")
        for i in range(1, n + 1):
            cat = random.choice(CATEGORIES)
            f.write(f"P{i:05d},San pham {i},{cat},Brand{random.randint(1, 40):02d},"
                    f"{round(random.uniform(20_000, 25_000_000), -3):.0f}\n")


def gen_transactions(path, rows, n_products=500, n_customers=200_000):
    start = dt.datetime(2025, 1, 1)
    with open(os.path.join(path, "retail_transactions.csv"), "w", encoding="utf-8") as f:
        f.write("transaction_id,customer_id,product_id,category,quantity,unit_price,"
                "discount,store_id,region,ts\n")
        for i in range(rows):
            # Phân phối khách hàng lệch: 20% khách tạo ~60% giao dịch (quy luật Pareto)
            cid = random.randint(1, n_customers // 5) if random.random() < 0.6 \
                else random.randint(1, n_customers)
            ts = start + dt.timedelta(seconds=random.randint(0, 240 * 86_400))
            price = round(random.uniform(20_000, 25_000_000), -3)
            f.write(f"T{i:010d},C{cid:07d},P{random.randint(1, n_products):05d},"
                    f"{random.choice(CATEGORIES)},{random.randint(1, 5)},{price:.0f},"
                    f"{random.choice([0, 0, 0, 0.05, 0.1, 0.2, 0.3])},"
                    f"S{random.randint(1, 60):03d},{random.choice(REGIONS)},"
                    f"{ts.strftime('%Y-%m-%d %H:%M:%S')}\n")
            if i % 500_000 == 0 and i:
                print(f"  giao dich: {i:,}")


def gen_weblog(path, rows):
    start = dt.datetime(2025, 3, 1)
    with open(os.path.join(path, "web_access.log"), "w", encoding="utf-8") as f:
        for i in range(rows):
            ip = f"{random.randint(14, 220)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"
            ts = start + dt.timedelta(seconds=random.randint(0, 30 * 86_400))
            url = random.choice(URLS)
            f.write(f'{ip} - - [{ts.strftime("%d/%b/%Y:%H:%M:%S +0700")}] '
                    f'"GET {url} HTTP/1.1" {random.choice(STATUS)} {random.randint(180, 98_000)} '
                    f'"-" "{random.choice(AGENTS)}"\n')


def gen_sensors(path, rows, n_devices=2000):
    start = dt.datetime(2025, 6, 1)
    with open(os.path.join(path, "sensor_readings.csv"), "w", encoding="utf-8") as f:
        f.write("device_id,ts,temperature,humidity,pm25,battery\n")
        for i in range(rows):
            ts = start + dt.timedelta(seconds=i % 86_400 + (i // 86_400) * 86_400)
            f.write(f"D{random.randint(1, n_devices):05d},{ts.strftime('%Y-%m-%d %H:%M:%S')},"
                    f"{round(random.uniform(18, 41), 1)},{round(random.uniform(35, 95), 1)},"
                    f"{round(random.uniform(5, 180), 1)},{random.randint(20, 100)}\n")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=5_000_000, help="số dòng giao dịch")
    ap.add_argument("--out", default="./data")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--small", action="store_true", help="chỉ sinh bản rút gọn")
    a = ap.parse_args()
    random.seed(a.seed)
    os.makedirs(a.out, exist_ok=True)
    rows = min(a.rows, 200_000) if a.small else a.rows
    print(f"Sinh du lieu vao {a.out} ({rows:,} giao dich)...")
    gen_products(a.out)
    gen_transactions(a.out, rows)
    gen_weblog(a.out, max(rows // 5, 50_000))
    gen_sensors(a.out, max(rows // 10, 50_000))
    for name in ("products.csv", "retail_transactions.csv", "web_access.log", "sensor_readings.csv"):
        p = os.path.join(a.out, name)
        print(f"  {name}: {os.path.getsize(p) / 1e6:.1f} MB")
