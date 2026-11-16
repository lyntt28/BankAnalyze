# BankAnalyze
A repo for Data Engineering class in HCMUS. We crawled daily stock prices and seasonal finance metrics from 16 Vietnam Banks to visualize by Tableau

## Cài đặt

Cần Python 3.9+, Firefox và [geckodriver](https://github.com/mozilla/geckodriver/releases) trong `PATH`.

```bash
pip install -r requirements.txt
cp .env.example .env   # điền INSTANCE_CONNECTION_NAME, DB_USER, DB_PASS
```

Đăng nhập Google Cloud để Cloud SQL Connector dùng được: `gcloud auth application-default login`.

## Chạy

Crawl (kết quả ghi vào `results/`, gộp tăng dần với file CSV đã có):

| Script | Nguồn | Tần suất | File |
|---|---|---|---|
| `stock_history.py` | investing.com | hằng ngày | `stock_history.csv` |
| `no-nhom-theo-quy.py`, `no-nhom-theo-nam.py` | cafef | theo quý | `no-nhom-theo-*.csv` |
| `thong-tin-tai-chinh-theo-quy.py`, `thong-tin-tai-chinh-theo-nam.py` | cafef | theo quý | `thong-tin-tai-chinh-theo-*.csv` |
| `shareholder.py` | VNDirect | theo quý | `co-dong-*.csv` (snapshot) |

Đẩy lên database (upsert theo khoá, không xoá bảng):

```bash
python update_daily.py      # stock_history -> DB DAILY
python update_seasonal.py   # các bảng còn lại -> DB SEASONAL
```

## Cấu trúc

- `config.py` – danh sách ngân hàng, URL, hằng số
- `utils.py` – Selenium driver/wait, parse dòng, gộp CSV tăng dần
- `_cafef_common.py` – logic crawl dùng chung cho các trang cafef
- `db.py` – kết nối Cloud SQL (đọc thông tin từ `.env`) và upsert
