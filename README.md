# BankAnalyze
A repo for Data Engineering class in HCMUS. We crawled daily stock prices and seasonal finance metrics from 16 Vietnam Banks to visualize by Tableau

Ngân hàng: ACB, BID, CTG, EIB, HDB, LPB, MBB, MSB, OCB, STB, SSB, TCB, TPB, VCB, VIB, VPB.

## Nguồn dữ liệu

Tất cả lấy từ API JSON mà trang [cafef.vn/du-lieu](https://cafef.vn/du-lieu) dùng để hiển thị,
nên không cần trình duyệt/Selenium. (Trước đây dùng Selenium với s.cafef.vn, investing.com và
VNDirect; s.cafef.vn đã chuyển trang, investing.com chặn bot.)

| Script | Dữ liệu | File | Khoá |
|---|---|---|---|
| `stock_history.py` | Giá OHLC, khối lượng, % thay đổi hằng ngày | `stock_history.csv` | Date, Bank |
| `no-nhom-theo-quy.py` / `-nam.py` | Nợ nhóm 1–5 (tỷ đồng) | `no-nhom-theo-*.csv` | season/year, group, bank |
| `thong-tin-tai-chinh-theo-quy.py` / `-nam.py` | Doanh thu, LNTT, chi phí, LN ròng (tỷ đồng) | `thong-tin-tai-chinh-theo-*.csv` | season/year, group, bank |
| `shareholder.py` | Cổ đông lớn; tỷ lệ sở hữu nước ngoài / nhà nước (snapshot) | `co-dong-*.csv` | — |

Định dạng kỳ: năm `2025`, quý `3-2025`. Giá tính bằng nghìn đồng, `Value` bằng tỷ đồng.

## Chạy

```bash
pip install -r requirements.txt
python stock_history.py                 # chỉ lấy phần mới (tự bù các ngày bị lỡ)
python no-nhom-theo-quy.py              # 8 quý gần nhất, gộp vào CSV cũ
python no-nhom-theo-quy.py --full       # toàn bộ lịch sử
python shareholder.py --banks ACB VCB   # chỉ một số ngân hàng
```

Mỗi lần chạy gộp dữ liệu mới vào CSV đã có: kỳ trùng thì lấy số mới nhất, kỳ cũ được giữ.
Nếu một ngân hàng lỗi, các ngân hàng khác vẫn chạy và script thoát với mã lỗi 1.

### Đẩy lên database (Cloud SQL MySQL)

```bash
cp .env.example .env    # điền INSTANCE_CONNECTION_NAME, DB_USER, DB_PASS
gcloud auth application-default login
python update_daily.py      # stock_history -> DB DAILY (upsert theo Date, Bank)
python update_seasonal.py   # các bảng còn lại -> DB SEASONAL
```

Lần đầu chuyển sang code mới, nên chạy với `--rebuild` để thay toàn bộ bảng cũ
(dữ liệu cũ lưu số dạng chuỗi `"1,23"` và dùng mã `LVB` thay vì `LPB`).

## Tự động cập nhật (GitHub Actions)

`.github/workflows/update-data.yml`:
- Thứ 2–6, 17:17 giờ VN: crawl giá ngày, commit `results/`.
- Thứ 2, 08:43 giờ VN: crawl dữ liệu theo quý/năm và cổ đông.
- Chạy tay: tab **Actions → Update data → Run workflow** (chọn `daily`/`seasonal`/`all`, `--full`).

Nếu thêm các secret `GCP_SA_KEY`, `INSTANCE_CONNECTION_NAME`, `DB_USER`, `DB_PASS` thì workflow
tự đẩy dữ liệu lên database; không có thì chỉ cập nhật CSV.

`ci.yml` chạy test (`pytest tests`) trên mỗi lần push.

## Cấu trúc

- `config.py` – danh sách ngân hàng, endpoint, hằng số
- `utils.py` – gọi API (retry/backoff), gộp CSV tăng dần, CLI
- `cafef_periodic.py` – logic chung cho dữ liệu theo quý/năm
- `db.py` – kết nối Cloud SQL (thông tin từ biến môi trường) và upsert
- `tests/` – test offline
