"""Đẩy results/stock_history.csv lên database DAILY.

Mặc định upsert theo (Date, Bank). --rebuild: thay toàn bộ bảng bằng CSV
(dùng một lần sau khi crawl --full, để dọn dữ liệu định dạng cũ / mã LVB).
"""
import argparse

import pandas as pd

from db import get_engine, replace, upsert
from utils import log, result_path

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--rebuild', action='store_true')
args = p.parse_args()

data = pd.read_csv(result_path('stock_history.csv'))
engine = get_engine("DAILY")
if args.rebuild:
    replace(engine, 'stock_history', data)
else:
    upsert(engine, 'stock_history', data, ['Date', 'Bank'])
log.info("stock_history: %d dòng", len(data))
