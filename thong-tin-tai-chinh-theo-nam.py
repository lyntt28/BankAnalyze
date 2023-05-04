from selenium import webdriver
import pandas as pd
from selenium.webdriver.common.by import By
import re
from unidecode import unidecode
from time import sleep, gmtime, strftime
from tqdm import tqdm
from ast import literal_eval

BANKS_CODE = ['ACB', 'BID', 'CTG', 'EIB', 'HDB', 'LVB', 'MBB', 'MSB', 'OCB', 'STB', 'SSB', 'TCB', 'TPB', 'VCB', 'VIB', 'VPB']
BANKS_URL  = [
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
BANKS_DICT= dict(zip(BANKS_URL,BANKS_CODE))
CONVERT_DEBT_GROUP = {
    'no-du-tieu-chuan': 'nhom-1',
'no-can-chu-y': 'nhom-2',
'no-duoi-tieu-chuan': 'nhom-3',
'no-nghi-ngo': 'nhom-4', 
'no-co-kha-nang-mat-von': 'nhom-5', 
}
REMOVE_LIST = ['(Đã kiểm toán)', 
              'Xem đầy đủ',
              'Lãi ròng từ hoạt động tín dụng',
             'Lãi ròng từ HĐ KD ngoại hối, vàng',
             'Lãi thuần từ đầu tư, KD chứng khoán',
             'Lãi thuần từ hoạt động khác',
             '(*) tỷ đồng',
               'Chỉ tiêu',
               'Kết quả kinh doanh',
               'Tài sản',
               '(*): Bao gồm doanh thu thuần hàng hóa & dịch vụ, doanh thu tài chính và doanh thu khác',
 '(**): Trừ LNST của cổ đông thiểu số (nếu có)']
CURRENT_YEAR = int(strftime("%Y", gmtime()))
CURRENT_MONTH = int(strftime("%m", gmtime()))
driver = webdriver.Firefox()

scraped_table = pd.DataFrame()
for crawl_url in tqdm(BANKS_URL):
    driver.get(crawl_url)
    sleep(2)
    driver.execute_script("window.scrollTo(0, 2500)")
    sleep(2)
    index_path = '//*[@id="divHoSoCongTyAjax"]'
    element_index = driver.find_element(By.XPATH, index_path)
    text = element_index.text
    lines = text.split('\n')
    lines = [i for i in lines if i not in REMOVE_LIST]
    years = lines[:4]
    years = [i.replace('Năm ', '') for i in years]
    if len(scraped_table)<1:
        scraped_table = pd.DataFrame(columns=['feature', 'bank']+years)
    for line in lines[4:]:
        feature_name = ''
        format_line = []
        line_split = line.split(' ')
        for j in line_split:
            j = j.strip()
            if ',' not in j:
                feature_name = f'{feature_name} {j}'
            else:
                if len(feature_name)==0:
                    format_line.append(j)
                else:
                    feature_name = feature_name.strip()
                    feature_name = unidecode(feature_name).replace(' ','-').lower()
                    format_line.append(feature_name)
                    format_line.append(BANKS_DICT.get(crawl_url))
                    format_line.append(j)
                    feature_name = ''
        try:
            scraped_table.loc[len(scraped_table)] = format_line
        except:
            continue
driver.close()
transform_table = pd.DataFrame(columns=["year", "value", "group", "bank"])
lastest_year = transform_table['year'].max()
years_found = list(map(int, scraped_table.columns[2:]))
for year in tqdm(years_found):
    if type(lastest_year)==int and year<=lastest_year:
        continue
    else:
        adding_table = scraped_table[[str(year), 'feature',  'bank']]
        adding_table.insert(0,'year_happend',[year]*len(adding_table))
        adding_table.columns = transform_table.columns
        transform_table = pd.concat([transform_table, adding_table],axis=0)
transform_table.to_csv('results/thong-tin-tai-chinh-theo-nam.csv', index=False)