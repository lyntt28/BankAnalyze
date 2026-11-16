"""Cơ cấu cổ đông lớn & tỷ lệ sở hữu nước ngoài/nhà nước (nguồn: cafef).

Đây là dữ liệu snapshot: mỗi lần chạy ghi lại toàn bộ, kèm ngày crawl.
"""
from datetime import date

import pandas as pd

from config import BANKS_CODE
from utils import cafef_get, cli, finish, for_each_bank, log, result_path

OWNER_TYPES = {'NuocNgoai': 'nuoc-ngoai', 'NhaNuoc': 'nha-nuoc', 'Khac': 'khac'}


def vn_number(text):
    """'203.077.983' -> 203077983 ; '3,5' -> 3.5"""
    if text is None or text == '':
        return None
    return float(str(text).replace('.', '').replace(',', '.'))


def fetch(bank):
    data = cafef_get('CoCauSoHuu.ashx', Symbol=bank)
    holders = [[h['Name'], vn_number(h['AssetVolume']), vn_number(h['AssetRate']),
                h['UpdatedDate'], bank] for h in data.get('CoDongSoHuu') or []]
    owners = [[label, data.get(key), bank] for key, label in OWNER_TYPES.items()]
    log.info("%s: %d cổ đông lớn", bank, len(holders))
    return holders, owners


def main():
    args = cli(__doc__)
    results, failed = for_each_bank(args.banks or BANKS_CODE, fetch)
    today = date.today().isoformat()
    holders = pd.DataFrame([h for r in results for h in r[0]],
                           columns=['shareholder_name', 'number', 'own_(%)',
                                    'updated_date', 'Bank'])
    owners = pd.DataFrame([o for r in results for o in r[1]],
                          columns=['metric', 'own_(%)', 'Bank'])
    holders['crawl_date'] = owners['crawl_date'] = today
    if failed:
        # Không ghi đè snapshot cũ bằng dữ liệu thiếu ngân hàng
        finish(failed)
    holders.to_csv(result_path('co-dong-tong-quan.csv'), index=False)
    owners.to_csv(result_path('co-dong-nuoc-ngoai.csv'), index=False)
    log.info("Đã ghi %d cổ đông, %d dòng cơ cấu sở hữu", len(holders), len(owners))


if __name__ == '__main__':
    main()
