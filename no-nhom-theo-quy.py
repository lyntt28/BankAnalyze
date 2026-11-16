from cafef_periodic import QUARTER, run
from utils import cli

if __name__ == '__main__':
    run('debt', QUARTER, cli(__doc__ or 'no-nhom theo quy'))
