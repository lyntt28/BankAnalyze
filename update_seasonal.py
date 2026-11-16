"""Đẩy các bảng theo kỳ & cổ đông lên database SEASONAL.

Bảng theo kỳ: upsert theo khoá (hoặc thay toàn bộ với --rebuild).
Bảng cổ đông là snapshot: luôn thay toàn bộ.
"""
import argparse

import pandas as pd

from db import get_engine, replace, upsert
from utils import log, result_path

TABLES = {
    'no-nhom-theo-nam': ['year', 'group', 'bank'],
    'no-nhom-theo-quy': ['season', 'group', 'bank'],
    'thong-tin-tai-chinh-theo-nam': ['year', 'group', 'bank'],
    'thong-tin-tai-chinh-theo-quy': ['season', 'group', 'bank'],
    'co-dong-tong-quan': None,
    'co-dong-nuoc-ngoai': None,
}

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--rebuild', action='store_true')
args = p.parse_args()

engine = get_engine("SEASONAL")
for name, keys in TABLES.items():
    try:
        data = pd.read_csv(result_path(f'{name}.csv'))
    except FileNotFoundError:
        log.warning("Không có %s.csv, bỏ qua", name)
        continue
    if keys and not args.rebuild:
        upsert(engine, name, data, keys)
    else:
        replace(engine, name, data)
    log.info("%s: %d dòng", name, len(data))
