from cafef_periodic import YEAR, run
from utils import cli

if __name__ == '__main__':
    run('debt', YEAR, cli(__doc__ or 'no-nhom theo nam'))
