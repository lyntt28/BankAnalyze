"""Logic chung cho các script crawl cafef (nợ nhóm & thông tin tài chính)."""
import pandas as pd
from tqdm import tqdm

from config import BANKS, cafef_url
from utils import (add_row, log, make_driver, merge_incremental,
                   polite_pause, result_path,
                   wait_text_change, wait_xpath)


def crawl(container_xpath, extract, tab_xpath=None, scroll=2000):
    """Duyệt 16 ngân hàng, trả về list các DataFrame dạng dài (long).

    `extract(text, bank_code)` -> (periods, rows); mỗi row = [feature, bank, v1..vn].
    """
    driver = make_driver()
    frames = []
    try:
        for bank in tqdm(BANKS):
            code = bank['code']
            try:
                polite_pause()
                driver.get(cafef_url(bank))
                driver.execute_script(f"window.scrollTo(0, {scroll})")
                container = wait_xpath(driver, container_xpath)
                text = container.text
                if tab_xpath:
                    wait_xpath(driver, tab_xpath, clickable=True).click()
                    text = wait_text_change(driver, container_xpath, text)
                periods, rows = extract(text, code)
            except Exception:
                log.exception("Lỗi khi crawl %s, bỏ qua ngân hàng này", code)
                continue
            wide = pd.DataFrame(columns=['feature', 'bank'] + periods)
            for row in rows:
                add_row(wide, row, f"{code}")
            frames.append(wide.melt(id_vars=['feature', 'bank'],
                                    var_name='period', value_name='value'))
    finally:
        driver.quit()
    if not frames:
        raise RuntimeError("Không crawl được ngân hàng nào")
    return pd.concat(frames, ignore_index=True)


def save(long_df, out_name, period_col, columns):
    """long_df có cột feature/bank/period/value -> lưu theo thứ tự `columns`."""
    df = long_df.rename(columns={'period': period_col, 'feature': 'group'})
    df = df.dropna(subset=['group'])[columns]
    merge_incremental(result_path(out_name), df, [period_col, 'group', 'bank'])
