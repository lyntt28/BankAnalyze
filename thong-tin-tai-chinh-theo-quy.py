from cafef_periodic import QUARTER, run
from utils import cli

if __name__ == '__main__':
    run('finance', QUARTER, cli(__doc__ or 'thong-tin-tai-chinh theo quy'))
