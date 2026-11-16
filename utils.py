"""Hàm dùng chung: gọi API cafef, log, gộp CSV tăng dần."""
import argparse
import logging
import os
import random
import time

import pandas as pd
import requests
from unidecode import unidecode

from config import (CAFEF_API, HTTP_RETRIES, HTTP_TIMEOUT, POLITE_DELAY,
                    RESULTS_DIR)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bankanalyze")

_session = requests.Session()
_session.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Referer": "https://cafef.vn/",
})


def cafef_get(endpoint, **params):
    """GET một endpoint JSON của cafef, có retry + backoff. Trả về trường Data."""
    url = f"{CAFEF_API}/{endpoint}"
    for attempt in range(1, HTTP_RETRIES + 1):
        time.sleep(random.uniform(*POLITE_DELAY))
        try:
            r = _session.get(url, params=params, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            body = r.json()
            if not body.get("Success", True):
                raise ValueError(f"API báo lỗi: {body.get('Message')}")
            return body["Data"]
        except Exception as e:
            if attempt == HTTP_RETRIES:
                raise
            wait = 2 ** attempt
            log.warning("%s %s lỗi (%s), thử lại sau %ds", endpoint, params, e, wait)
            time.sleep(wait)


def for_each_bank(banks, fn):
    """Chạy fn(bank) cho từng ngân hàng; lỗi ở một ngân hàng không làm dừng cả lượt.

    Trả về (list kết quả, list ngân hàng lỗi).
    """
    results, failed = [], []
    for bank in banks:
        try:
            results.append(fn(bank))
        except Exception:
            log.exception("%s: lỗi, bỏ qua", bank)
            failed.append(bank)
    return results, failed


def slug(text):
    text = unidecode(str(text)).replace('(', ' ').replace(')', ' ').replace('*', ' ')
    return '-'.join(text.lower().split())


def result_path(name):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    return os.path.join(RESULTS_DIR, name)


def merge_incremental(path, new_data, key_cols, sort_cols=None):
    """Gộp dữ liệu mới vào CSV cũ.

    Dòng trùng khoá thì lấy bản mới (số liệu có thể được kiểm toán lại), dòng
    cũ không còn trên web vẫn được giữ.
    """
    if os.path.exists(path):
        old = pd.read_csv(path)
        log.info("%s: đã có %d dòng", path, len(old))
    else:
        old = pd.DataFrame(columns=new_data.columns)
    for c in key_cols:  # so khoá dạng chuỗi để tránh lệch kiểu int/str
        old[c] = old[c].astype(str)
        new_data[c] = new_data[c].astype(str)
    merged = (pd.concat([old, new_data], ignore_index=True)
              .drop_duplicates(subset=key_cols, keep='last'))
    if sort_cols:
        merged = merged.sort_values(sort_cols, kind='stable')
    merged.to_csv(path, index=False)
    log.info("%s: +%d dòng mới, tổng %d", path, len(merged) - len(old), len(merged))
    return merged


def finish(failed):
    """Thoát với mã lỗi nếu có ngân hàng crawl thất bại (để CI báo đỏ)."""
    if failed:
        log.error("Các ngân hàng lỗi: %s", ", ".join(failed))
        raise SystemExit(1)


def cli(description):
    p = argparse.ArgumentParser(description=description)
    p.add_argument('--full', action='store_true',
                   help='lấy toàn bộ lịch sử thay vì chỉ các kỳ gần nhất')
    p.add_argument('--banks', nargs='+', help='chỉ chạy các mã này')
    return p.parse_args()
