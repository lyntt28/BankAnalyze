import random as rd
import re
from time import sleep

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from tqdm import tqdm

STOCK_CRAWL_URLS = """
https://vn.investing.com/equities/asia-commercial-bank-historical-data
https://vn.investing.com/equities/commercial-bank-investment-develop-historical-data
https://vn.investing.com/equities/vietnam-joint-stock-commercial-bank-historical-data
https://vn.investing.com/equities/vietnam-export-import-commercial-historical-data
https://vn.investing.com/equities/ho-chi-minh-city-develop-historical-data
https://vn.investing.com/equities/lien-viet-post-historical-data
https://vn.investing.com/equities/military-commercial-bank-historical-data
https://vn.investing.com/equities/vietnam-maritime-commercial-historical-data
https://vn.investing.com/equities/orient-commercial-joint-stock-bank-historical-data
https://vn.investing.com/equities/sai-gon-thuong-tin-commercial-historical-data
https://vn.investing.com/equities/southeast-asia-commercial-bank-historical-data
https://vn.investing.com/equities/vtc-bank-historical-data
https://vn.investing.com/equities/tien-phong-commercial-historical-data
https://vn.investing.com/equities/joint-stock-commercial-bank-historical-data
https://vn.investing.com/equities/viet-nam-international-commercial-j-historical-data
https://vn.investing.com/equities/vietnam-prosperity-historical-data
""".split()
BANKS_CODE = ['ACB', 'BID', 'CTG', 'EIB', 'HDB',
              'LVB', 'MBB', 'MSB', 'OCB', 'STB',
              'SSB', 'TCB', 'TPB', 'VCB', 'VIB',
              'VPB']
LINK_TO_BANK = dict(zip(STOCK_CRAWL_URLS, BANKS_CODE))
driver = webdriver.Firefox()


def update_dataframe(text: str, stock_history: pd.DataFrame):
    lines = text.split('\n')
    lines = [i for i in lines if len(re.findall('\d', i)) != 0][:-1]
    for line in lines:
        format_line = []
        for i in line.split(' '):
            if len(i) > 0:
                if ')' not in i and '(' not in i:
                    format_line.append(i)
                else:
                    format_line[-1] += i
        stock_history.loc[len(stock_history)] = format_line
    return stock_history


stock_history = pd.DataFrame(columns=[
    'Date', 'Close', 'Open', 'Highest',
    'Lowest', 'KL',
    'Change(%)', 'Bank'
])
for crawl_url in tqdm(STOCK_CRAWL_URLS):
    sleep(rd.randint(15, 30))
    driver.get(crawl_url)
    sleep(rd.randint(15, 30))
    driver.execute_script("window.scrollTo(0, 1000)")
    sleep(rd.randint(15, 30))
    xpath_path = '//*[@id="__next"]'
    element = driver.find_element(By.XPATH, xpath_path)
    text_split = element.text.split('\n')
    lines = [i for i in text_split if '/2023' in i and ',' in i]
    for line in lines[1:2]:
        format_line = line.split()
        format_line.append(LINK_TO_BANK.get(crawl_url))
        stock_history.loc[len(stock_history)] = format_line
driver.close()


def KL_format(kl_str: str):
    if 'M' in kl_str:
        return float(kl_str.replace('M', ''))*10**6
    elif 'K' in kl_str:
        return float(kl_str.replace('K', ''))*10**3
    else:
        return float(kl_str)


def float_format(float_value):
    float_value = float_value.replace('%', '').replace(',', '')
    return float(float_value)


combine_data = pd.read_csv('results/stock_history.csv')
combine_data['Date'] = pd.to_datetime(combine_data["Date"], format='%Y/%m/%d')
data = stock_history.copy()
data['KL'] = data['KL'].apply(KL_format)
data['Close'] = data['Close'].apply(float_format)
data['Open'] = data['Open'].apply(float_format)
data['Highest'] = data['Highest'].apply(float_format)
data['Lowest'] = data['Lowest'].apply(float_format)
data['Change(%)'] = data['Change(%)'].apply(float_format)
data['Date'] = pd.to_datetime(data["Date"], format='%d/%m/%Y')
if data['Date'].max() <= combine_data['Date'].max():
    print('Stock History is already updated')
else:
    combine_data = pd.concat([combine_data, data], axis=0)
    combine_data.to_csv('results/stock_history.csv', index=False)
