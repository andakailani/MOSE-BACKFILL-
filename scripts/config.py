"""
CONFIG untuk MOSE backfill multi-tahun.

Basis: CONFIG dict di MOSE Notebook 01 (§3.3 dokumen laporan final).
PERUBAHAN vs versi 2024-only:
  1. PRICE_MIN/PRICE_MAX dilebarkan (lihat catatan di bawah) — perbaikan
     bug laten, bukan kosmetik.
  2. SESSION_HOURS ditambahkan — mengisi gap #1 dari laporan MOSE §9.1
     (kolom sesi hilang saat resample tick->bar). Dihitung sekali di sini,
     di level tick, supaya tidak perlu retrofit mahal nanti.
"""

CONFIG = {
    "PRICE_DIVISOR": 100000,          # histdata raw price / divisor = harga desimal asli
    "DT_FORMAT_RAW": "%Y%m%d %H%M%S%f",
    "CHUNK_SIZE": 500_000,            # baris per chunk saat streaming read & tulis parquet
                                       # (dipakai langsung oleh ingest_month.py sekarang,
                                       # supaya RAM runner tidak menampung 1 bulan penuh
                                       # sekaligus - lihat catatan di ingest_month.py)

    # ------------------------------------------------------------------
    # PRICE_MIN/PRICE_MAX di dokumen asli (1500.0 / 4000.0) dikalibrasi
    # HANYA untuk data 2024. Harga gold pernah serendah ~US$250 (2001)
    # dan ~US$1050 (2015-2016). Ini filter garbage-data (harga negatif,
    # nol, angka rusak feed), BUKAN filter outlier ekonomi.
    # ------------------------------------------------------------------
    "PRICE_MIN": 200.0,
    "PRICE_MAX": 4000.0,

    "OUTLIER_SIGMA": 5.0,             # dihitung per-tahun di stage ini;
                                       # konsolidasi lintas-tahun ada di
                                       # scripts/consolidate_outliers.py
                                       # (BELUM DIBANGUN - lihat catatan §gap
                                       # di review sebelumnya. Prioritas
                                       # setelah backfill 1-2 tahun pertama
                                       # sukses, bukan blocker untuk mulai).
    "MAX_SPREAD": 5.0,
    "MIN_SPREAD": 0.05,
    "SMOOTH_WINDOW": 3,
    "GAP_THRESHOLD_MIN": 60,

    "TIMEFRAMES": {
        "M1": "1min", "M5": "5min", "M15": "15min",
        "H1": "1h", "H4": "4h", "D1": "1D",
    },

    # ------------------------------------------------------------------
    # SESSION_HOURS — batas jam untuk kolom `session` di clean_chunk().
    # ASUMSI PENTING yang WAJIB dicek manual saat uji 1 bulan pertama:
    # histdata.com generic ASCII biasanya memakai jam EST tanpa DST
    # (bukan UTC). Batas di bawah ditulis dalam jam RAW dari file
    # (0-23), bukan UTC. Kalau setelah uji coba ternyata datanya UTC
    # atau timezone lain, geser angka jam di bawah ini seperlunya -
    # jangan diasumsikan benar tanpa dicek.
    # Non-overlapping by design (tiap jam cuma masuk 1 kategori) supaya
    # groupby/agregasi sederhana; overlap London-NY yang sesungguhnya
    # ekonomis bisa didekati lewat kategori "Overlap" terpisah.
    # ------------------------------------------------------------------
    "SESSION_HOURS": [
        # (nama, jam_mulai_inklusif, jam_selesai_eksklusif) - jam raw 0-23
        ("Asian",    0, 8),
        ("London",   8, 13),
        ("Overlap", 13, 16),
        ("NY",      16, 22),
        ("Asian",   22, 24),   # bungkus balik ke sesi Asia berikutnya
    ],

    "PAIR": "xauusd",
    # Bucket Backblaze B2 (bukan Drive) - independen dari MOSE.
    # Konsolidasi ke MOSE dilakukan manual belakangan, keputusan sadar.
    "B2_BUCKET": "xauusd-backfill-andakailani-2013-2023",
    "B2_BASE_PATH": "01_parquet",
    "TARGET_YEARS": list(range(2013, 2024)),  # 2013..2023 inklusif
}
