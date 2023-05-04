from selenium import webdriver
from tqdm import tqdm
from selenium.webdriver.common.by import By
import pandas as pd
from tqdm import tqdm
from time import sleep
import re
import random as rd
from unidecode import unidecode
from bs4 import BeautifulSoup  

BANKS_CODE = ['ACB', 'BID', 'CTG', 'EIB', 'HDB', 
              'LVB', 'MBB', 'MSB', 'OCB', 'STB', 
              'SSB', 'TCB', 'TPB', 'VCB', 'VIB', 
              'VPB']

driver = webdriver.Firefox()
global_shareholder_df = pd.DataFrame()
global_foreign_df = pd.DataFrame()
for bank in tqdm(BANKS_CODE):
    if bank=='LVB':
        bank = 'LPB'
    sleep(rd.randint(15,30))
    crawl_url = f"https://dstock.vndirect.com.vn/tong-quan/{bank}/co-cau-so-huu-popup?tabDefault=shareholders"
    driver.get(crawl_url)
    sleep(rd.randint(15,30))
    driver.execute_script("window.scrollTo(0, 2300)") 
    sleep(rd.randint(15,30))
    
    shareholder_df = pd.DataFrame(columns=['shareholder_name', 'number', 'own_(%)'])
    html_text = driver.page_source
    soup = BeautifulSoup(html_text, "html.parser")
    text_elements = soup.find_all('td', class_='text-left')
    values_elements = soup.find_all('td', class_='nowrap')
    flag = 0
    for shareholder in tqdm(text_elements):
        shareholder_name = shareholder.get_text()
        number = values_elements[flag].get_text()
        flag+=1
        own_number = values_elements[flag].get_text().replace('%','')
        flag+=1
        shareholder_df.loc[len(shareholder_df)] = [shareholder_name, number, own_number]
    xpath_path = """Sở hữu NN"""
    element = driver.find_element(By.LINK_TEXT, xpath_path)
    element.click()
    sleep(rd.randint(15,30))
    shareholder_df['Bank'] = bank
    
    html_text = driver.page_source
    soup = BeautifulSoup(html_text, "html.parser")
    text_elements = soup.find_all('td', class_='text-left')
    values_elements = soup.find_all('td', class_='nowrap')
    text_elements = [i.get_text() for i in text_elements]
    values_elements = [i.get_text().replace('%','') for i in values_elements]
    foreign_df = pd.DataFrame.from_dict({'metric': text_elements, 'own_(%)': values_elements})
    foreign_df['Bank'] = bank
    
    global_shareholder_df = pd.concat([global_shareholder_df, shareholder_df], axis=0)
    global_foreign_df = pd.concat([global_foreign_df, foreign_df], axis=0)
    global_foreign_df.to_csv('results/co-dong-nuoc-ngoai.csv', index=False)
    global_shareholder_df.to_csv('results/co-dong-tong-quan.csv', index=False)
driver.close()