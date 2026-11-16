"""Crawl cơ cấu cổ đông & sở hữu nước ngoài từ VNDirect (dữ liệu snapshot)."""
import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from tqdm import tqdm

from config import BANKS, WAIT_TIMEOUT, vndirect_url
from utils import log, make_driver, polite_pause, result_path


def cells(html):
    soup = BeautifulSoup(html, "html.parser")
    return ([td.get_text() for td in soup.find_all('td', class_='text-left')],
            [td.get_text() for td in soup.find_all('td', class_='nowrap')])


shareholders, foreigners = [], []
driver = make_driver()
wait = WebDriverWait(driver, WAIT_TIMEOUT)
try:
    for bank in tqdm(BANKS):
        code = bank['code']
        try:
            polite_pause()
            driver.get(vndirect_url(bank))
            driver.execute_script("window.scrollTo(0, 2300)")
            first = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, 'td.text-left')))

            names, values = cells(driver.page_source)
            if len(values) < 2 * len(names):
                log.warning("%s: thiếu giá trị cổ đông (%d tên, %d ô)",
                            code, len(names), len(values))
            for i, name in enumerate(names[:len(values) // 2]):
                shareholders.append([name, values[2 * i],
                                     values[2 * i + 1].replace('%', ''), code])

            wait.until(EC.element_to_be_clickable(
                (By.LINK_TEXT, "Sở hữu NN"))).click()
            wait.until(EC.staleness_of(first))
            wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, 'td.text-left')))
            names, values = cells(driver.page_source)
            for name, value in zip(names, values):
                foreigners.append([name, value.replace('%', ''), code])
        except Exception:
            log.exception("Lỗi khi crawl %s", code)
finally:
    driver.quit()

pd.DataFrame(shareholders, columns=['shareholder_name', 'number', 'own_(%)', 'Bank']) \
    .to_csv(result_path('co-dong-tong-quan.csv'), index=False)
pd.DataFrame(foreigners, columns=['metric', 'own_(%)', 'Bank']) \
    .to_csv(result_path('co-dong-nuoc-ngoai.csv'), index=False)
