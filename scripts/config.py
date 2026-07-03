"""
CONFIG untuk MOSE backfill multi-tahun.

Basis: CONFIG dict di MOSE Notebook 01 (§3.3 dokumen laporan final).
PERUBAHAN PENTING vs versi 2024-only — lihat catatan PRICE_MIN/PRICE_MAX di
bawah. Ini BUKAN penyesuaian kosmetik, ini perbaikan bug laten yang baru
kelihatan begitu kamu keluar dari data 2024.
"""

CONFIG = {
    "PRICE_DIVISOR": 100000,          # histdata raw price / divisor = harga desimal asli
    "DT_FORMAT_RAW": "%Y%m%d %H%M%S%f",
    "CHUNK_SIZE": 500_000,            # baris per chunk saat streaming read

    # ------------------------------------------------------------------
    # PENTING — PRICE_MIN/PRICE_MAX di dokumen asli (1500.0 / 4000.0)
    # dikalibrasi HANYA untuk data 2024, di mana harga gold memang di
    # rentang itu. Begitu kamu ingest tahun 2004-2019, harga gold pernah
    # serendah ~US$250 (2001) dan ~US$1050 (2015-2016). Kalau bound lama
    # dipakai, chunk-chunk itu akan ditolak validate_chunk() bukan
    # sebagai outlier, tapi sebagai "data rusak" — padahal valid.
    #
    # Ini filter garbage-data (harga negatif, nol, angka rusak feed),
    # BUKAN filter outlier ekonomi — outlier ekonomi tetap ditangani
    # terpisah oleh global z-score pass (§3.5), yang MELABELI, bukan
    # membuang. Jadi aman untuk dilebarkan jauh di sini.
    # ------------------------------------------------------------------
    "PRICE_MIN": 200.0,
    "PRICE_MAX": 4000.0,

    "OUTLIER_SIGMA": 5.0,             # dihitung per-tahun di stage ini;
                                       # konsolidasi lintas-tahun ada di
                                       # scripts/consolidate_outliers.py
    "MAX_SPREAD": 5.0,                # sedikit dilonggarkan dari 3.0 — spread
                                       # bisa lebih lebar di era likuiditas
                                       # lebih rendah (pra-2010). Tinjau ulang
                                       # setelah lihat distribusi aktual.
    "MIN_SPREAD": 0.05,
    "SMOOTH_WINDOW": 3,
    "GAP_THRESHOLD_MIN": 60,

    "TIMEFRAMES": {
        "M1": "1min", "M5": "5min", "M15": "15min",
        "H1": "1h", "H4": "4h", "D1": "1D",
    },

    "PAIR": "xauusd",
    # PENTING — sengaja TERPISAH dari folder MOSE di akun Drive lain.
    # Project ini independen: kumpulkan + validasi 2013-2023 dulu di sini,
    # konsolidasi ke MOSE/ (akun nawibarrak) dilakukan manual belakangan,
    # sebagai keputusan sadar, bukan otomatis.
    "DRIVE_BASE": "XAUUSD_Backfill_2013_2023/data",
    "TARGET_YEARS": list(range(2013, 2024)),  # 2013..2023 inklusif
}
