import pandas as pd
from tqdm import tqdm

from db import get_engine, replace, upsert
from utils import log, result_path

# Bảng theo kỳ -> upsert theo khoá; bảng cổ đông là snapshot -> replace.
TABLES = {
    'no-nhom-theo-nam': ['year', 'group', 'bank'],
    'no-nhom-theo-quy': ['season', 'group', 'bank'],
    'thong-tin-tai-chinh-theo-nam': ['year', 'group', 'bank'],
    'thong-tin-tai-chinh-theo-quy': ['season', 'group', 'bank'],
    'co-dong-tong-quan': None,
    'co-dong-nuoc-ngoai': None,
}

engine = get_engine("SEASONAL")
for name, keys in tqdm(TABLES.items()):
    try:
        data = pd.read_csv(result_path(f'{name}.csv'))
    except FileNotFoundError:
        log.warning("Không có %s.csv, bỏ qua", name)
        continue
    if keys:
        upsert(engine, name, data, keys)
    else:
        replace(engine, name, data)
    log.info("%s: %d dòng", name, len(data))
