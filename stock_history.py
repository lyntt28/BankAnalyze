"""Lịch sử giá cổ phiếu hằng ngày của 16 ngân hàng (nguồn: cafef).

Mặc định: lấy từ ngày cuối cùng đã có trong CSV (lùi lại 7 ngày để chắc chắn)
tới hôm nay, nên lỡ vài ngày không chạy vẫn tự bù. Lần đầu (chưa có CSV) lấy
1 năm gần nhất; dùng --full để lấy toàn bộ lịch sử từ 2010.
"""
import os
import re
from datetime import date, datetime, timedelta

import pandas as pd

from config import BANKS_CODE
from utils import cafef_get, cli, finish, for_each_bank, log, merge_incremental, result_path

PATH = result_path('stock_history.csv')
COLUMNS = ['Date', 'Close', 'Open', 'Highest', 'Lowest', 'KL', 'Change(%)',
           'AdjClose', 'Value', 'Bank']
FULL_START = date(2010, 1, 1)
PCT_RE = re.compile(r'\(\s*([-+]?[\d.,]+)\s*%\s*\)')


def parse_pct(text):
    m = PCT_RE.search(text or '')
    return float(m.group(1).replace(',', '.')) if m else None


def fetch_range(bank, start, end):
    """Lấy mọi phiên trong [start, end], tự phân trang."""
    rows, page = [], 1
    while True:
        data = cafef_get('DataHistory/PriceHistory.ashx', Symbol=bank,
                         StartDate=start.strftime('%m/%d/%Y'),
                         EndDate=end.strftime('%m/%d/%Y'),
                         PageIndex=page, PageSize=30)
        items = data.get('Data') or []
        for it in items:
            rows.append([
                datetime.strptime(it['Ngay'], '%d/%m/%Y').strftime('%Y-%m-%d'),
                it['GiaDongCua'], it['GiaMoCua'], it['GiaCaoNhat'], it['GiaThapNhat'],
                it['KhoiLuongKhopLenh'], parse_pct(it['ThayDoi']),
                it['GiaDieuChinh'], it['GiaTriKhopLenh'], bank,
            ])
        if not items or len(rows) >= (data.get('TotalCount') or 0):
            return rows
        page += 1


def last_dates():
    if not os.path.exists(PATH):
        return {}
    old = pd.read_csv(PATH, usecols=['Date', 'Bank'])
    old['Date'] = pd.to_datetime(old['Date'], format='mixed')
    return old.groupby('Bank')['Date'].max().dt.date.to_dict()


def fetch_bank(bank, full, known):
    today = date.today()
    if full:
        start = FULL_START
    elif bank in known:
        start = known[bank] - timedelta(days=7)
    else:
        start = today - timedelta(days=365)
    rows = []
    # Chia theo năm để mỗi lượt phân trang ngắn
    while start <= today:
        end = min(date(start.year, 12, 31), today)
        rows += fetch_range(bank, start, end)
        start = end + timedelta(days=1)
    log.info("%s: %d phiên", bank, len(rows))
    return rows


def main():
    args = cli(__doc__)
    known = last_dates()
    results, failed = for_each_bank(args.banks or BANKS_CODE,
                                    lambda b: fetch_bank(b, args.full, known))
    df = pd.DataFrame([r for rows in results for r in rows], columns=COLUMNS)
    if not df.empty:
        merge_incremental(PATH, df, ['Date', 'Bank'], sort_cols=['Bank', 'Date'])
    finish(failed)


if __name__ == '__main__':
    main()
