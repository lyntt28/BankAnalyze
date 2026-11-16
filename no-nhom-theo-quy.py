from _cafef_common import crawl, save
from config import CONVERT_DEBT_GROUP
from utils import parse_feature_line


def extract(text, bank):
    lines = text.split('\n')
    seasons = lines[1].replace('Quý', '').split()
    rows = [parse_feature_line(line, bank,
                               lambda t: ',' in t or '--' in t,
                               CONVERT_DEBT_GROUP)
            for line in lines[2:7]]
    return seasons, rows


data = crawl('//*[@id="NLoaded"]', extract)
save(data, 'no-nhom-theo-quy.csv', 'season', ["season", "value", "group", "bank"])
