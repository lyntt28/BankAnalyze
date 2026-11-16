"""Hàm dùng chung: driver, chờ phần tử, parse dòng, gộp dữ liệu tăng dần."""
import logging
import os
import random
import re
from time import sleep

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from unidecode import unidecode

from config import POLITE_DELAY, RESULTS_DIR, WAIT_TIMEOUT

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("bankanalyze")


def make_driver():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    return webdriver.Firefox()


def polite_pause():
    sleep(random.uniform(*POLITE_DELAY))


def wait_xpath(driver, xpath, clickable=False):
    cond = EC.element_to_be_clickable if clickable else EC.presence_of_element_located
    return WebDriverWait(driver, WAIT_TIMEOUT).until(cond((By.XPATH, xpath)))


def wait_text_change(driver, xpath, old_text):
    """Chờ nội dung phần tử đổi sau khi bấm tab (AJAX). Tìm lại phần tử mỗi
    lần kiểm tra vì trang có thể thay thế cả node."""
    def changed(d):
        try:
            text = d.find_element(By.XPATH, xpath).text
        except Exception:
            return False
        return text if text.strip() and text != old_text else False
    return WebDriverWait(driver, WAIT_TIMEOUT).until(changed)


def result_path(name):
    return os.path.join(RESULTS_DIR, name)


def parse_feature_line(line, bank, is_value, name_map=None):
    """Tách một dòng 'Tên chỉ tiêu 1,23 4,56 ...' thành [feature, bank, v1, v2...].

    Trả về None nếu dòng không có giá trị nào.
    """
    feature_name = ''
    out = []
    for token in line.split(' '):
        token = token.strip()
        if not is_value(token):
            feature_name = f'{feature_name} {token}'
        elif not feature_name.strip():
            out.append(token)
        else:
            key = unidecode(feature_name.strip()).replace(' ', '-').lower()
            out += [name_map.get(key) if name_map else key, bank, token]
            feature_name = ''
    return out or None


def add_row(table, row, context):
    """Thêm dòng vào bảng; nếu số cột không khớp thì log thay vì bỏ qua im lặng."""
    if row is None:
        return
    if len(row) != len(table.columns):
        log.warning("Bỏ dòng %s: có %d giá trị, cần %d: %r",
                    context, len(row), len(table.columns), row)
        return
    table.loc[len(table)] = row


def merge_incremental(path, new_data, key_cols):
    """Gộp dữ liệu mới vào CSV cũ.

    Dòng trùng khoá thì lấy bản mới (số liệu có thể được kiểm toán lại), dòng
    cũ không còn hiển thị trên web vẫn được giữ.
    """
    if os.path.exists(path):
        old = pd.read_csv(path, dtype=str)
        log.info("%s: đã có %d dòng", path, len(old))
    else:
        old = pd.DataFrame(columns=new_data.columns)
    new_data = new_data.astype(str)
    merged = (pd.concat([old, new_data], ignore_index=True)
              .drop_duplicates(subset=key_cols, keep='last'))
    added = len(merged) - len(old)
    merged.to_csv(path, index=False)
    log.info("%s: +%d dòng mới, tổng %d", path, added, len(merged))
    return merged


DATE_RE = re.compile(r'^\d{2}/\d{2}/\d{4}\b')
