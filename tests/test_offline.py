"""Test không cần mạng: giả lập API cafef bằng monkeypatch."""
import pandas as pd
import pytest

import cafef_periodic
import config
import shareholder
import stock_history
import utils


@pytest.fixture(autouse=True)
def tmp_results(tmp_path, monkeypatch):
    monkeypatch.setattr(config, 'RESULTS_DIR', str(tmp_path))
    monkeypatch.setattr(utils, 'RESULTS_DIR', str(tmp_path))
    monkeypatch.setattr(utils, 'POLITE_DELAY', (0, 0))
    return tmp_path


def test_merge_incremental_keeps_old_and_updates(tmp_path):
    path = tmp_path / 'x.csv'
    utils.merge_incremental(str(path), pd.DataFrame(
        {'season': ['1-2023', '2-2023'], 'value': [1.0, 2.0], 'bank': ['ACB'] * 2}),
        ['season', 'bank'])
    out = utils.merge_incremental(str(path), pd.DataFrame(
        {'season': ['2-2023', '3-2023'], 'value': [2.5, 3.0], 'bank': ['ACB'] * 2}),
        ['season', 'bank'])
    assert dict(zip(out['season'], out['value'])) == {
        '1-2023': 1.0, '2-2023': 2.5, '3-2023': 3.0}


def test_debt_quarter_paging(monkeypatch):
    calls = []

    def fake(endpoint, **p):
        calls.append(p)
        y, q = p['FromYear'], p['FromQuarter']
        periods = []
        for _ in range(4):
            q = q - 1 if q > 1 else 4
            y = y - 1 if q == 4 else y
            periods.append({'Year': y, 'Quarter': q, 'Values': [
                {'Code': 'TM_CLN_001', 'Name': 'Nợ đủ tiêu chuẩn', 'Value': 2e9},
                {'Code': 'TM_LA', 'Name': 'lạ', 'Value': 1}]})
        return {'Values': periods, 'HasOlder': True}

    monkeypatch.setattr(cafef_periodic, 'cafef_get', fake)
    rows = cafef_periodic.fetch_debt('ACB', cafef_periodic.QUARTER, full=False)
    assert len(rows) == config.RECENT_QUARTERS          # bỏ mã lạ
    assert len({r[0] for r in rows}) == config.RECENT_QUARTERS  # không trùng kỳ
    assert rows[0][1:] == [2.0, 'nhom-1', 'ACB']         # đổi sang tỷ đồng
    assert len(calls) == 2


def test_finance_units_and_slug(monkeypatch):
    monkeypatch.setattr(cafef_periodic, 'cafef_get', lambda e, **p: {'Value': [
        {'Year': 2025, 'Quater': 0, 'Value': [
            {'Name': 'Lợi nhuận ròng(**)', 'Value': 15624699000}]}]})
    rows = cafef_periodic.fetch_finance('ACB', cafef_periodic.YEAR, full=False)
    assert rows == [['2025', 'loi-nhuan-rong', pytest.approx(15624.699), 'ACB']]


def test_stock_parse_and_paging(monkeypatch):
    item = {'Ngay': '13/11/2026', 'GiaDongCua': 20.35, 'GiaMoCua': 20.3,
            'GiaCaoNhat': 20.5, 'GiaThapNhat': 20.3, 'KhoiLuongKhopLenh': 100,
            'ThayDoi': '-0,05 (-0,25%)', 'GiaDieuChinh': 20.35, 'GiaTriKhopLenh': 2.0}
    pages = {1: [item] * 30, 2: [item] * 5}
    monkeypatch.setattr(stock_history, 'cafef_get', lambda e, **p: {
        'TotalCount': 35, 'Data': pages.get(p['PageIndex'], [])})
    from datetime import date
    rows = stock_history.fetch_range('ACB', date(2026, 1, 1), date(2026, 12, 31))
    assert len(rows) == 35
    assert rows[0][0] == '2026-11-13' and rows[0][6] == -0.25


def test_vn_number():
    assert shareholder.vn_number('203.077.983') == 203077983
    assert shareholder.vn_number('3,5') == 3.5
    assert shareholder.vn_number('') is None


def test_for_each_bank_isolates_errors():
    def fn(b):
        if b == 'BAD':
            raise RuntimeError('x')
        return b
    ok, failed = utils.for_each_bank(['ACB', 'BAD', 'VCB'], fn)
    assert ok == ['ACB', 'VCB'] and failed == ['BAD']
