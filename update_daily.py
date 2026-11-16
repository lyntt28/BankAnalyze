import pandas as pd

from db import get_engine, upsert
from utils import log, result_path

data = pd.read_csv(result_path('stock_history.csv'))
n = upsert(get_engine("DAILY"), 'stock_history', data, ['Date', 'Bank'])
log.info("stock_history: upsert %d dòng", n)
