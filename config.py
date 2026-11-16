"""Cấu hình dùng chung cho toàn bộ crawler."""

RESULTS_DIR = "results"

# Một nguồn duy nhất cho danh sách ngân hàng (mã niêm yết hiện tại).
# LienVietPostBank dùng LPB ở mọi nơi (trước đây có chỗ ghi LVB).
BANKS_CODE = ['ACB', 'BID', 'CTG', 'EIB', 'HDB',
              'LPB', 'MBB', 'MSB', 'OCB', 'STB',
              'SSB', 'TCB', 'TPB', 'VCB', 'VIB',
              'VPB']

# API JSON mà trang cafef.vn/du-lieu dùng để hiển thị dữ liệu.
CAFEF_API = "https://cafef.vn/du-lieu/Ajax/PageNew"

DEBT_GROUP_CODE = {
    'TM_CLN_001': 'nhom-1',  # Nợ đủ tiêu chuẩn
    'TM_CLN_002': 'nhom-2',  # Nợ cần chú ý
    'TM_CLN_003': 'nhom-3',  # Nợ dưới tiêu chuẩn
    'TM_CLN_004': 'nhom-4',  # Nợ nghi ngờ
    'TM_CLN_005': 'nhom-5',  # Nợ có khả năng mất vốn
}

# Số kỳ gần nhất lấy mỗi lần chạy thường. Chạy với --full để lấy toàn bộ lịch sử.
RECENT_QUARTERS = 8
RECENT_YEARS = 4

HTTP_TIMEOUT = 30
HTTP_RETRIES = 4
POLITE_DELAY = (0.5, 1.5)   # giây nghỉ giữa các request
