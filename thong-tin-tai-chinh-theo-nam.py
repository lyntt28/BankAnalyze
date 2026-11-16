from _cafef_common import crawl, save
from config import FINANCE_REMOVE_LIST
from utils import parse_feature_line


def extract(text, bank):
    lines = [l for l in text.split('\n') if l not in FINANCE_REMOVE_LIST]
    years = [l.replace('Năm ', '').strip() for l in lines[:4]]
    rows = [parse_feature_line(line, bank, lambda t: ',' in t)
            for line in lines[4:]]
    return years, rows


data = crawl('//*[@id="divHoSoCongTyAjax"]', extract, scroll=2500)
save(data, 'thong-tin-tai-chinh-theo-nam.csv', 'year',
     ["year", "value", "group", "bank"])
