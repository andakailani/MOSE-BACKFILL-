"""
Ingest satu bulan data XAUUSD dari histdata.com, proses jadi parquet,
simpan lokal di runner (nanti di-upload ke Drive oleh workflow).

Dipanggil per-bulan (bukan per-tahun) supaya:
  1. Muat dalam batas waktu 1 job GitHub Actions (~6 jam, tapi harusnya
     jauh lebih cepat dari itu per bulan).
  2. Kalau 1 bulan gagal, tidak perlu ulang 1 tahun penuh.
  3. Selaras dengan struktur partisi zero-padded yang sudah kamu pakai
     (year=YYYY/month=MM).

CATATAN JUJUR — belum diuji end-to-end (sandbox development ini tidak
punya akses jaringan). Sebelum full backfill 10 tahun, WAJIB jalankan
dulu untuk 1 bulan lewat workflow_dispatch dan cek hasilnya manual.
Kemungkinan yang perlu disesuaikan:
  - Nama instrumen di package `histdata` untuk XAUUSD (kadang perlu
    dicoba 'xauusd' vs kode lain tergantung update situs).
  - histdata.com kadang menaruh proteksi anti-bot tambahan yang bikin
    package butuh update. Kalau gagal, Dukascopy (lewat `duka` atau
    `dukascopy-node`) adalah fallback yang sudah disiapkan strukturnya
    di download_source() di bawah.
"""

import argparse
import io
import sys
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

from config import CONFIG

RAW_TEMP_DIR = Path("/tmp/mose_temp/raw_incoming")
REJECTED_DIR = Path("/tmp/mose_temp/rejected_rows")
OUT_DIR = Path("/tmp/mose_out")


def download_source(year: int, month: int) -> Path:
    """
    Unduh 1 bulan tick data XAUUSD dari histdata.com.
    Return path ke file CSV mentah (sudah diekstrak dari ZIP).

    Primary: package `histdata` (pip install histdata).
    Fallback manual kalau package gagal / instrumen tidak ketemu:
    unduh ZIP langsung dari URL histdata.com generic ASCII tick data
    dan proses sama seperti biasa.
    """
    RAW_TEMP_DIR.mkdir(parents=True, exist_ok=True)

    try:
        from histdata import download_hist_data as dl
        from histdata.api import Platform as P, TimeFrame as TF

        zip_path = dl(
            year=str(year),
            month=str(month),
            pair=CONFIG["PAIR"],
            platform=P.GENERIC_ASCII,
            time_frame=TF.TICK_DATA,
            output_directory=str(RAW_TEMP_DIR),
        )
        return Path(zip_path)
    except Exception as e:  # noqa: BLE001 — sengaja luas, ini fallback path
        print(f"[WARN] package histdata gagal ({e}); coba fallback manual", file=sys.stderr)
        raise NotImplementedError(
            "Fallback manual histdata.com / Dukascopy belum diisi — "
            "isi di sini setelah uji coba 1 bulan menunjukkan package utama gagal."
        )


def parse_tick_csv(zip_path: Path) -> pd.DataFrame:
    """Baca ZIP histdata, kembalikan DataFrame tick mentah."""
    with zipfile.ZipFile(zip_path) as zf:
        csv_name = [n for n in zf.namelist() if n.lower().endswith(".csv")][0]
        with zf.open(csv_name) as f:
            df = pd.read_csv(
                f,
                sep=",",
                header=None,
                names=["dt_raw", "bid", "ask", "vol"],
                dtype={"dt_raw": str},
            )
    return df


def validate_chunk(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Filter sesuai §3.4 step 2. Return (valid, rejected)."""
    df = df.copy()
    df["timestamp"] = pd.to_datetime(
        df["dt_raw"], format=CONFIG["DT_FORMAT_RAW"], errors="coerce"
    )
    df["mid"] = (df["bid"] + df["ask"]) / 2
    df["spread"] = df["ask"] - df["bid"]

    mask_valid = (
        df["timestamp"].notna()
        & df["mid"].between(CONFIG["PRICE_MIN"], CONFIG["PRICE_MAX"])
        & df["spread"].between(CONFIG["MIN_SPREAD"], CONFIG["MAX_SPREAD"])
    )
    return df[mask_valid].copy(), df[~mask_valid].copy()


def clean_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """§3.4 step 3: dedup, smoothing, kolom turunan."""
    df = df.sort_values("timestamp").drop_duplicates("timestamp", keep="last")
    df["mid_smooth"] = df["mid"].ewm(span=CONFIG["SMOOTH_WINDOW"]).mean()
    df["hour"] = df["timestamp"].dt.hour
    df["year"] = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month
    df["spread_pips"] = df["spread"] / 0.10
    return df


def write_partition(df: pd.DataFrame, year: int, month: int) -> Path:
    out_path = OUT_DIR / "01_parquet" / "tick" / f"year={year:04d}" / f"month={month:02d}"
    out_path.mkdir(parents=True, exist_ok=True)
    file_path = out_path / "part-0000.parquet"
    df.to_parquet(file_path, index=False)
    return file_path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--month", type=int, required=True)
    args = ap.parse_args()

    print(f"[INFO] Ingest XAUUSD {args.year}-{args.month:02d}")
    zip_path = download_source(args.year, args.month)
    raw = parse_tick_csv(zip_path)
    valid, rejected = validate_chunk(raw)
    clean = clean_chunk(valid)

    REJECTED_DIR.mkdir(parents=True, exist_ok=True)
    if len(rejected):
        rejected.to_parquet(
            REJECTED_DIR / f"rejected_{args.year}{args.month:02d}.parquet", index=False
        )

    out_file = write_partition(clean, args.year, args.month)

    nat_count = int(raw["dt_raw"].isna().sum())
    print(
        f"[OK] {len(clean):,} baris bersih | {len(rejected):,} ditolak | "
        f"NaT={nat_count} | output={out_file}"
    )

    # exit non-zero kalau kosong total — supaya job GitHub Actions gagal
    # dengan jelas, bukan diam-diam sukses tanpa data
    if len(clean) == 0:
        print("[FAIL] Tidak ada baris valid untuk bulan ini.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
