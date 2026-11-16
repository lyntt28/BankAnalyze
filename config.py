"""Cấu hình dùng chung cho toàn bộ crawler."""

RESULTS_DIR = "results"

# Một nguồn duy nhất cho danh sách ngân hàng. LienVietPostBank dùng mã LPB
# (mã niêm yết hiện tại) ở mọi nơi.
BANKS = [
    {"code": "ACB", "cafef": "hose/ACB-ngan-hang-thuong-mai-co-phan-a-chau.chn",
     "investing": "asia-commercial-bank"},
    {"code": "BID", "cafef": "hose/BID-ngan-hang-thuong-mai-co-phan-dau-tu-va-phat-trien-viet-nam.chn",
     "investing": "commercial-bank-investment-develop"},
    {"code": "CTG", "cafef": "hose/CTG-ngan-hang-thuong-mai-co-phan-cong-thuong-viet-nam.chn",
     "investing": "vietnam-joint-stock-commercial-bank"},
    {"code": "EIB", "cafef": "hose/EIB-ngan-hang-thuong-mai-co-phan-xuat-nhap-khau-viet-nam.chn",
     "investing": "vietnam-export-import-commercial"},
    {"code": "HDB", "cafef": "hose/HDB-ngan-hang-tmcp-phat-trien-tp-ho-chi-minh.chn",
     "investing": "ho-chi-minh-city-develop"},
    {"code": "LPB", "cafef": "hose/LPB-ngan-hang-thuong-mai-co-phan-buu-dien-lien-viet.chn",
     "investing": "lien-viet-post"},
    {"code": "MBB", "cafef": "hose/MBB-ngan-hang-thuong-mai-co-phan-quan-doi.chn",
     "investing": "military-commercial-bank"},
    {"code": "MSB", "cafef": "hose/MSB-ngan-hang-thuong-mai-co-phan-hang-hai-viet-nam.chn",
     "investing": "vietnam-maritime-commercial"},
    {"code": "OCB", "cafef": "hose/OCB-ngan-hang-thuong-mai-co-phan-phuong-dong.chn",
     "investing": "orient-commercial-joint-stock-bank"},
    {"code": "STB", "cafef": "hose/STB-ngan-hang-thuong-mai-co-phan-sai-gon-thuong-tin.chn",
     "investing": "sai-gon-thuong-tin-commercial"},
    {"code": "SSB", "cafef": "hose/SSB-ngan-hang-thuong-mai-co-phan-dong-nam-a.chn",
     "investing": "southeast-asia-commercial-bank"},
    {"code": "TCB", "cafef": "hose/TCB-ngan-hang-tmcp-ky-thuong-viet-nam-techcombank.chn",
     "investing": "vtc-bank"},
    {"code": "TPB", "cafef": "hose/TPB-ngan-hang-thuong-mai-co-phan-tien-phong.chn",
     "investing": "tien-phong-commercial"},
    {"code": "VCB", "cafef": "hose/VCB-ngan-hang-thuong-mai-co-phan-ngoai-thuong-viet-nam.chn",
     "investing": "joint-stock-commercial-bank"},
    {"code": "VIB", "cafef": "hose/VIB-ngan-hang-thuong-mai-co-phan-quoc-te-viet-nam.chn",
     "investing": "viet-nam-international-commercial-j"},
    {"code": "VPB", "cafef": "hose/VPB-ngan-hang-thuong-mai-co-phan-viet-nam-thinh-vuong.chn",
     "investing": "vietnam-prosperity"},
]

BANKS_CODE = [b["code"] for b in BANKS]


def cafef_url(bank):
    return f"https://s.cafef.vn/{bank['cafef']}"


def investing_url(bank):
    return f"https://vn.investing.com/equities/{bank['investing']}-historical-data"


def vndirect_url(bank):
    return (f"https://dstock.vndirect.com.vn/tong-quan/{bank['code']}"
            "/co-cau-so-huu-popup?tabDefault=shareholders")


CONVERT_DEBT_GROUP = {
    'no-du-tieu-chuan': 'nhom-1',
    'no-can-chu-y': 'nhom-2',
    'no-duoi-tieu-chuan': 'nhom-3',
    'no-nghi-ngo': 'nhom-4',
    'no-co-kha-nang-mat-von': 'nhom-5',
}

FINANCE_REMOVE_LIST = [
    '(Đã kiểm toán)',
    'Xem đầy đủ',
    'Lãi ròng từ hoạt động tín dụng',
    'Lãi ròng từ HĐ KD ngoại hối, vàng',
    'Lãi thuần từ đầu tư, KD chứng khoán',
    'Lãi thuần từ hoạt động khác',
    '(*) tỷ đồng',
    'Chỉ tiêu',
    'Kết quả kinh doanh',
    'Tài sản',
    '(*): Bao gồm doanh thu thuần hàng hóa & dịch vụ, doanh thu tài chính và doanh thu khác',
    '(**): Trừ LNST của cổ đông thiểu số (nếu có)',
]

# Thời gian chờ tối đa (giây) cho WebDriverWait, và khoảng nghỉ lịch sự giữa
# các request để không bị chặn.
WAIT_TIMEOUT = 30
POLITE_DELAY = (1, 3)
