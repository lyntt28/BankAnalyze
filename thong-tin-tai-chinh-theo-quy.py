from _cafef_common import crawl, save
from config import FINANCE_REMOVE_LIST
from utils import parse_feature_line


def extract(text, bank):
    lines = [l for l in text.split('\n') if l not in FINANCE_REMOVE_LIST]
    seasons = [s.replace('Quý', '').strip() for s in lines[0].split(' Quý ')]
    rows = [parse_feature_line(line, bank, lambda t: ',' in t)
            for line in lines[1:]]
    return seasons, rows


data = crawl('//*[@id="divHoSoCongTyAjax"]', extract,
             tab_xpath='//*[@id="litimetabs1"]/a', scroll=2500)
save(data, 'thong-tin-tai-chinh-theo-quy.csv', 'season',
     ["season", "group", "value", "bank"])
