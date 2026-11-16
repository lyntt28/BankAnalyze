from cafef_periodic import YEAR, run
from utils import cli

if __name__ == '__main__':
    run('finance', YEAR, cli(__doc__ or 'thong-tin-tai-chinh theo nam'))
