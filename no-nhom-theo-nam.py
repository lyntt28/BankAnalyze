import random as rd
from time import gmtime, sleep, strftime

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from tqdm import tqdm
from unidecode import unidecode

BANKS_CODE = ['ACB', 'BID', 'CTG', 'EIB', 'HDB',
              'LVB', 'MBB', 'MSB', 'OCB', 'STB',
              'SSB', 'TCB', 'TPB', 'VCB', 'VIB',
              'VPB']
BANKS_URL = [
    "https://s.cafef.vn/hose/ACB-ngan-hang-thuong-mai-co-phan-a-chau.chn",
    "https://s.cafef.vn/hose/BID-ngan-hang-thuong-mai-co-phan-dau-tu-va-phat-trien-viet-nam.chn",
    "https://s.cafef.vn/hose/CTG-ngan-hang-thuong-mai-co-phan-cong-thuong-viet-nam.chn",
    "https://s.cafef.vn/hose/EIB-ngan-hang-thuong-mai-co-phan-xuat-nhap-khau-viet-nam.chn",
    "https://s.cafef.vn/hose/HDB-ngan-hang-tmcp-phat-trien-tp-ho-chi-minh.chn",
    "https://s.cafef.vn/hose/LPB-ngan-hang-thuong-mai-co-phan-buu-dien-lien-viet.chn",
    "https://s.cafef.vn/hose/MBB-ngan-hang-thuong-mai-co-phan-quan-doi.chn",
    "https://s.cafef.vn/hose/MSB-ngan-hang-thuong-mai-co-phan-hang-hai-viet-nam.chn",
    "https://s.cafef.vn/hose/OCB-ngan-hang-thuong-mai-co-phan-phuong-dong.chn",
    "https://s.cafef.vn/hose/STB-ngan-hang-thuong-mai-co-phan-sai-gon-thuong-tin.chn",
    "https://s.cafef.vn/hose/SSB-ngan-hang-thuong-mai-co-phan-dong-nam-a.chn",
    "https://s.cafef.vn/hose/TCB-ngan-hang-tmcp-ky-thuong-viet-nam-techcombank.chn",
    "https://s.cafef.vn/hose/TPB-ngan-hang-thuong-mai-co-phan-tien-phong.chn",
    "https://s.cafef.vn/hose/VCB-ngan-hang-thuong-mai-co-phan-ngoai-thuong-viet-nam.chn",
    "https://s.cafef.vn/hose/VIB-ngan-hang-thuong-mai-co-phan-quoc-te-viet-nam.chn",
    "https://s.cafef.vn/hose/VPB-ngan-hang-thuong-mai-co-phan-viet-nam-thinh-vuong.chn"
]
BANKS_DICT = dict(zip(BANKS_URL, BANKS_CODE))
CONVERT_DEBT_GROUP = {
    'no-du-tieu-chuan': 'nhom-1',
    'no-can-chu-y': 'nhom-2',
    'no-duoi-tieu-chuan': 'nhom-3',
    'no-nghi-ngo': 'nhom-4',
    'no-co-kha-nang-mat-von': 'nhom-5',
}
CURRENT_YEAR = int(strftime("%Y", gmtime()))
CURRENT_MONTH = int(strftime("%m", gmtime()))
driver = webdriver.Firefox()

scraped_table = pd.DataFrame()
for crawl_url in tqdm(BANKS_URL):
    sleep(rd.randint(5, 15))
    driver.get(crawl_url)
    sleep(rd.randint(5, 15))
    driver.execute_script("window.scrollTo(0, 2000)")
    sleep(rd.randint(5, 15))
    year_xpath = '//*[@id="lsdetitab2"]'
    element = driver.find_element(By.XPATH, year_xpath)
    element.click()
    sleep(rd.randint(5, 15))
    index_path = '//*[@id="NLoaded"]'
    element_index = driver.find_element(By.XPATH, index_path)
    text = element_index.text
    lines = text.split('\n')
    years = lines[1].split()
    if len(scraped_table) < 1:
        scraped_table = pd.DataFrame(columns=['feature', 'bank']+years)
    for line in lines[2:7]:
        feature_name = ''
        format_line = []
        line_split = line.split(' ')
        for j in line_split:
            j = j.strip()
            if ',' not in j and '--' not in j:
                feature_name = f'{feature_name} {j}'
            else:
                if len(feature_name) == 0:
                    format_line.append(j)
                else:
                    feature_name = feature_name.strip()
                    feature_name = unidecode(
                        feature_name).replace(' ', '-').lower()
                    format_line.append(CONVERT_DEBT_GROUP.get(feature_name))
                    format_line.append(BANKS_DICT.get(crawl_url))
                    format_line.append(j)
                    feature_name = ''
        scraped_table.loc[len(scraped_table)] = format_line
driver.close()

transform_table = pd.DataFrame(columns=["year", "value", "group", "bank"])
lastest_year = transform_table['year'].max()
years_found = list(map(int, scraped_table.columns[2:]))
for year in tqdm(years_found):
    if type(lastest_year) == int and year <= lastest_year:
        continue
    else:
        adding_table = scraped_table[[str(year), 'feature',  'bank']]
        adding_table.insert(0, 'year_happend', [year]*len(adding_table))
        adding_table.columns = transform_table.columns
        transform_table = pd.concat([transform_table, adding_table], axis=0)
transform_table.to_csv('results/no-nhom-theo-nam.csv', index=False)
