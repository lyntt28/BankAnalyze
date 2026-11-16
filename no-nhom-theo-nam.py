from _cafef_common import crawl, save
from config import CONVERT_DEBT_GROUP
from utils import parse_feature_line


def extract(text, bank):
    lines = text.split('\n')
    years = lines[1].split()
    rows = [parse_feature_line(line, bank,
                               lambda t: ',' in t or '--' in t,
                               CONVERT_DEBT_GROUP)
            for line in lines[2:7]]
    return years, rows


data = crawl('//*[@id="NLoaded"]', extract, tab_xpath='//*[@id="lsdetitab2"]')
save(data, 'no-nhom-theo-nam.csv', 'year', ["year", "value", "group", "bank"])
