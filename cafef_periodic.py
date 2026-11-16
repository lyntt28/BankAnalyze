"""Logic chung cho dữ liệu theo kỳ (quý/năm) từ cafef: nợ nhóm & chỉ tiêu tài chính."""
from datetime import date

import pandas as pd

from config import BANKS_CODE, DEBT_GROUP_CODE, RECENT_QUARTERS, RECENT_YEARS
from utils import ApiError, cafef_get, finish, for_each_bank, log, merge_incremental, result_path, slug

QUARTER = 'quy'
YEAR = 'nam'


def _period_label(kind, year, quarter):
    # Giữ định dạng cũ: năm -> "2025", quý -> "3-2025"
    return str(year) if kind == YEAR else f"{quarter}-{year}"


def _prev(kind, year, quarter):
    if kind == YEAR:
        return year - 1, quarter
    return (year, quarter - 1) if quarter > 1 else (year - 1, 4)


def fetch_debt(bank, kind, full):
    """Nợ theo nhóm. API trả 4 kỳ/lần, lùi dần về quá khứ."""
    today = date.today()
    year, quarter = today.year, (today.month - 1) // 3 + 1
    limit = None if full else (RECENT_YEARS if kind == YEAR else RECENT_QUARTERS)
    rows, seen = [], set()
    while limit is None or len(seen) < limit:
        params = dict(Symbol=bank, Type=1, FromYear=year, FromQuarter=quarter)
        if kind == YEAR:
            params.update(TypeTime=2, FromQuarter=0)
        data = cafef_get('GetDataDebInfo.ashx', **params)
        periods = data.get('Values') or []
        new = [p for p in periods if (p['Year'], p['Quarter']) not in seen]
        if not new:
            break
        for p in new:
            seen.add((p['Year'], p['Quarter']))
            label = _period_label(kind, p['Year'], p['Quarter'])
            for v in p['Values']:
                group = DEBT_GROUP_CODE.get(v['Code'])
                if group is None:
                    log.warning("%s: mã nợ lạ %s (%s)", bank, v['Code'], v['Name'])
                    continue
                value = None if v['Value'] is None else v['Value'] / 1e9  # tỷ đồng
                rows.append([label, value, group, bank])
        if not data.get('HasOlder'):
            break
        oldest = min((p['Year'], p['Quarter']) for p in new)
        year, quarter = _prev(kind, oldest[0], oldest[1] if kind == QUARTER else 0)
    return rows


def fetch_finance(bank, kind, full):
    """Các chỉ tiêu chính (doanh thu, LNTT, chi phí, LN ròng).

    API báo lỗi nếu TotalRow vượt quá số kỳ có sẵn, nên lấy từng đợt và lùi
    EndDate dần về quá khứ.
    """
    today = date.today()
    year, quarter = today.year, (today.month - 1) // 3 + 1
    limit = None if full else (RECENT_YEARS if kind == YEAR else RECENT_QUARTERS)
    chunk = 20 if full else limit
    rows, seen = [], set()
    while limit is None or len(seen) < limit:
        end = str(year) if kind == YEAR else f"{quarter}-{year}"
        try:
            data = cafef_get('FinanceReport.ashx', Type=1, Symbol=bank, TotalRow=chunk,
                             EndDate=end, ReportType='NAM' if kind == YEAR else 'QUY',
                             Sort='DESC')
        except ApiError:
            if chunk > 1:
                chunk = max(1, chunk // 4)  # vượt quá số kỳ có sẵn: thử đợt nhỏ hơn
                continue
            if not seen:
                raise
            break  # đã tới kỳ cũ nhất có dữ liệu
        new = [p for p in data.get('Value') or [] if (p['Year'], p['Quater']) not in seen]
        if not new:
            break
        for p in new:
            seen.add((p['Year'], p['Quater']))
            label = _period_label(kind, p['Year'], p['Quater'])
            for v in p['Value']:
                # API trả đơn vị nghìn đồng -> đổi sang tỷ đồng
                value = None if v['Value'] is None else v['Value'] / 1e6
                rows.append([label, slug(v['Name']), value, bank])
        oldest = min((p['Year'], p['Quater']) for p in new)
        year, quarter = _prev(kind, oldest[0], oldest[1] if kind == QUARTER else 0)
    return rows


def run(dataset, kind, args):
    banks = args.banks or BANKS_CODE
    if dataset == 'debt':
        fetch, cols, name = fetch_debt, ['value', 'group'], 'no-nhom'
    else:
        fetch, cols, name = fetch_finance, ['group', 'value'], 'thong-tin-tai-chinh'
    period_col = 'year' if kind == YEAR else 'season'
    columns = [period_col] + cols + ['bank']

    results, failed = for_each_bank(banks, lambda b: fetch(b, kind, args.full))
    rows = [r for bank_rows in results for r in bank_rows]
    df = pd.DataFrame(rows, columns=[period_col, 'value', 'group', 'bank'] if dataset == 'debt'
                      else [period_col, 'group', 'value', 'bank'])[columns]
    log.info("%s-theo-%s: crawl được %d dòng từ %d ngân hàng",
             name, kind, len(df), len(banks) - len(failed))

    if kind == QUARTER:
        sort_key = df[period_col].str.split('-', expand=True)
        df = df.assign(_y=sort_key[1].astype(int), _q=sort_key[0].astype(int)) \
               .sort_values(['bank', '_y', '_q']).drop(columns=['_y', '_q'])
    if not df.empty:
        merge_incremental(result_path(f'{name}-theo-{kind}.csv'), df,
                          [period_col, 'group', 'bank'])
    finish(failed)
