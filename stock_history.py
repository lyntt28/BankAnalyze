"""Crawl lịch sử giá cổ phiếu từ investing.com.

Lấy toàn bộ các ngày đang hiển thị trên trang (không chỉ 1 ngày) rồi gộp với
CSV cũ theo (Date, Bank), nên lỡ vài ngày không chạy vẫn bù lại được.
"""
import pandas as pd
from tqdm import tqdm

from config import BANKS, investing_url
from utils import (DATE_RE, log, make_driver, merge_incremental,
                   polite_pause, result_path, wait_xpath)

COLUMNS = ['Date', 'Close', 'Open', 'Highest', 'Lowest', 'KL', 'Change(%)', 'Bank']


def kl_format(kl_str):
    kl_str = kl_str.replace(',', '')
    for suffix, mult in (('B', 1e9), ('M', 1e6), ('K', 1e3)):
        if suffix in kl_str:
            return float(kl_str.replace(suffix, '')) * mult
    return float(kl_str)


def float_format(value):
    return float(value.replace('%', '').replace(',', ''))


def parse_page(text, bank):
    rows = []
    for line in text.split('\n'):
        if not DATE_RE.match(line):
            continue
        parts = line.split()
        if len(parts) != len(COLUMNS) - 1:
            log.warning("%s: bỏ dòng không đúng định dạng: %r", bank, line)
            continue
        rows.append(parts + [bank])
    return rows


rows = []
driver = make_driver()
try:
    for bank in tqdm(BANKS):
        try:
            polite_pause()
            driver.get(investing_url(bank))
            driver.execute_script("window.scrollTo(0, 1000)")
            wait_xpath(driver, '//table//td[contains(text(), "/")]')
            bank_rows = parse_page(
                driver.find_element('xpath', '//*[@id="__next"]').text, bank['code'])
            log.info("%s: %d ngày", bank['code'], len(bank_rows))
            rows += bank_rows
        except Exception:
            log.exception("Lỗi khi crawl %s", bank['code'])
finally:
    driver.quit()

data = pd.DataFrame(rows, columns=COLUMNS)
data['KL'] = data['KL'].apply(kl_format)
for col in ['Close', 'Open', 'Highest', 'Lowest', 'Change(%)']:
    data[col] = data[col].apply(float_format)
data['Date'] = pd.to_datetime(data['Date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')

path = result_path('stock_history.csv')
merged = merge_incremental(path, data, ['Date', 'Bank'])
# Chuẩn hoá ngày cũ (có thể đang ở dạng YYYY/MM/DD) rồi sắp xếp lại
merged['Date'] = pd.to_datetime(merged['Date'], format='mixed').dt.strftime('%Y-%m-%d')
merged = merged.drop_duplicates(['Date', 'Bank'], keep='last').sort_values(['Bank', 'Date'])
merged.to_csv(path, index=False)
