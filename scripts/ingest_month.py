"""
Ingest satu bulan data XAUUSD dari histdata.com, proses jadi parquet,
simpan lokal di runner (nanti di-upload ke B2 oleh workflow).

PERUBAHAN vs versi sebelumnya (lihat alasan di tiap fungsi):
  1. parse_tick_csv() sekarang STREAMING (pakai chunksize), bukan baca
     1 bulan penuh ke memori sekaligus. Ini penting karena runner GitHub
     Actions standar cuma py 7GB RAM, dan kita tidak tahu pasti berapa
     besar file bulan-bulan sibuk di 10 tahun data - lebih aman diasumsikan
     bisa besar daripada menebak kecil.
  2. clean_chunk() sekarang menghasilkan kolom `session` (Asian/London/
     Overlap/NY) - mengisi gap #1 paling penting dari laporan MOSE §9.1.
  3. download_source() sekarang retry 3x dengan jeda, karena histdata.com/
     GitHub Actions sama-sama bisa gagal sesaat karena jaringan, bukan
     cuma karena instrumen tidak ada.
  4. Ditambahkan write_manifest() - laporan ringkas per bulan (row count,
     NaT count, %rejected) sesuai rekomendasi assertion §3.6 MOSE, supaya
     validate_year bisa cek ISI bukan cuma folder ADA.
  5. Dedup & smoothing tetap dilakukan PER CHUNK (bukan across seluruh
     bulan) demi hemat RAM. KETERBATASAN YANG PERLU DIKETAHUI: duplikat
     timestamp yang kebetulan terpisah di 2 chunk berbeda, atau EWM
     smoothing yang "reset" di awal tiap chunk, TIDAK tertangkap sempurna.
     Ini trade-off sadar (RAM vs ketelitian sempurna di batas chunk) -
     dampaknya kecil (candidate: <CHUNK_SIZE baris paling dekat batas per
     ~500rb baris), tapi dicatat di sini supaya bukan bug tersembunyi.

CATATAN JUJUR (masih berlaku) — belum diuji end-to-end dengan akses
jaringan nyata. WAJIB jalankan 1 bulan dulu lewat workflow_dispatch dan
cek manual sebelum full backfill.
"""

import argparse
import json
import sys
import time
import traceback
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

from config import CONFIG

RAW_TEMP_DIR = Path("/tmp/mose_temp/raw_incoming")
REJECTED_DIR = Path("/tmp/mose_temp/rejected_rows")
MANIFEST_DIR = Path("/tmp/mose_temp/manifest")
OUT_DIR = Path("/tmp/mose_out")

MAX_DOWNLOAD_ATTEMPTS = 3
RETRY_WAIT_SECONDS = 20


def download_source(year: int, month: int) -> Path:
    """
    Unduh 1 bulan tick data XAUUSD dari histdata.com, dengan retry.
    Return path ke file ZIP mentah.
    """
    RAW_TEMP_DIR.mkdir(parents=True, exist_ok=True)

    last_error = None
    for attempt in range(1, MAX_DOWNLOAD_ATTEMPTS + 1):
        try:
            from histdata import download_hist_data as dl
            from histdata.api import Platform as P, TimeFrame as TF

            print(f"[INFO] Download percobaan {attempt}/{MAX_DOWNLOAD_ATTEMPTS} "
                  f"untuk {year}-{month:02d} ...")
            zip_path = dl(
                year=str(year),
                month=str(month),
                pair=CONFIG["PAIR"],
                platform=P.GENERIC_ASCII,
                time_frame=TF.TICK_DATA,
                output_directory=str(RAW_TEMP_DIR),
            )
            return Path(zip_path)
        except Exception as e:  # noqa: BLE001 - sengaja luas, ini fallback/retry path
            last_error = e
            print(f"[WARN] Percobaan {attempt} gagal: {e}", file=sys.stderr)
            if attempt < MAX_DOWNLOAD_ATTEMPTS:
                time.sleep(RETRY_WAIT_SECONDS)

    # Semua percobaan gagal - jangan diam-diam lanjut dengan data kosong.
    # NOTE: fallback ke Dukascopy (duka / dukascopy-node) BELUM diisi.
    # Isi di sini setelah uji coba menunjukkan package `histdata` memang
    # tidak bisa diandalkan untuk instrumen/tahun tertentu.
    raise RuntimeError(
        f"Download gagal setelah {MAX_DOWNLOAD_ATTEMPTS}x percobaan untuk "
        f"{year}-{month:02d}. Error terakhir: {last_error}. "
        f"Fallback Dukascopy belum diimplementasikan."
    )


def iter_tick_chunks(zip_path: Path, chunk_size: int):
    """
    Generator: baca ZIP histdata secara STREAMING per chunk_size baris,
    supaya tidak perlu memuat 1 bulan penuh ke RAM sekaligus.
    """
    with zipfile.ZipFile(zip_path) as zf:
        csv_name = [n for n in zf.namelist() if n.lower().endswith(".csv")][0]
        with zf.open(csv_name) as f:
            reader = pd.read_csv(
                f,
                sep=",",
                header=None,
                names=["dt_raw", "bid", "ask", "vol"],
                dtype={"dt_raw": str},
                chunksize=chunk_size,
            )
            for chunk in reader:
                yield chunk


def validate_chunk(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Filter garbage-data. Return (valid, rejected)."""
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


def assign_session(hour_series: pd.Series) -> pd.Series:
    """
    Petakan jam (0-23, raw dari file - lihat catatan ASUMSI di config.py)
    ke label sesi. Non-overlapping by construction.
    """
    session = pd.Series("Unknown", index=hour_series.index, dtype="object")
    for name, start, end in CONFIG["SESSION_HOURS"]:
        mask = (hour_series >= start) & (hour_series < end)
        session[mask] = name
    return session


def clean_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """Dedup, smoothing, kolom turunan - termasuk `session` (baru)."""
    df = df.sort_values("timestamp").drop_duplicates("timestamp", keep="last")
    df["mid_smooth"] = df["mid"].ewm(span=CONFIG["SMOOTH_WINDOW"]).mean()
    df["hour"] = df["timestamp"].dt.hour
    df["year"] = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month
    df["spread_pips"] = df["spread"] / 0.10
    df["session"] = assign_session(df["hour"])
    return df


def write_manifest(year: int, month: int, stats: dict) -> Path:
    """Tulis laporan ringkas per bulan untuk dibaca validate_year."""
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
    path = MANIFEST_DIR / f"manifest_{year}{month:02d}.json"
    with open(path, "w") as f:
        json.dump(stats, f, indent=2)
    return path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--month", type=int, required=True)
    args = ap.parse_args()
    year, month = args.year, args.month

    print(f"[INFO] Ingest XAUUSD {year}-{month:02d}")

    stats = {
        "year": year, "month": month,
        "raw_rows": 0, "clean_rows": 0, "rejected_rows": 0,
        "nat_count": 0, "chunks_processed": 0,
        "status": "started",
    }

    try:
        zip_path = download_source(year, month)

        out_path = OUT_DIR / "01_parquet" / "tick" / f"year={year:04d}" / f"month={month:02d}"
        out_path.mkdir(parents=True, exist_ok=True)
        REJECTED_DIR.mkdir(parents=True, exist_ok=True)

        rejected_frames = []
        part_idx = 0

        for raw_chunk in iter_tick_chunks(zip_path, CONFIG["CHUNK_SIZE"]):
            stats["chunks_processed"] += 1
            stats["raw_rows"] += len(raw_chunk)
            stats["nat_count"] += int(
                pd.to_datetime(raw_chunk["dt_raw"], format=CONFIG["DT_FORMAT_RAW"],
                               errors="coerce").isna().sum()
            )

            valid, rejected = validate_chunk(raw_chunk)
            if len(rejected):
                rejected_frames.append(rejected)
                stats["rejected_rows"] += len(rejected)

            if len(valid) == 0:
                continue

            clean = clean_chunk(valid)
            stats["clean_rows"] += len(clean)

            # Tulis tiap chunk sebagai part file terpisah - ini yang membuat
            # proses ini hemat RAM (tidak perlu concat seluruh bulan dulu).
            part_file = out_path / f"part-{part_idx:04d}.parquet"
            clean.to_parquet(part_file, index=False)
            part_idx += 1
            print(f"[INFO] Chunk {stats['chunks_processed']}: "
                  f"{len(clean):,} baris bersih -> {part_file.name}")

        if rejected_frames:
            rej_all = pd.concat(rejected_frames, ignore_index=True)
            rej_all.to_parquet(
                REJECTED_DIR / f"rejected_{year}{month:02d}.parquet", index=False
            )

        if stats["clean_rows"] == 0:
            stats["status"] = "failed_empty"
            write_manifest(year, month, stats)
            print("[FAIL] Tidak ada baris valid untuk bulan ini.", file=sys.stderr)
            sys.exit(1)

        stats["status"] = "ok"
        write_manifest(year, month, stats)
        print(
            f"[OK] {stats['clean_rows']:,} baris bersih | "
            f"{stats['rejected_rows']:,} ditolak | NaT={stats['nat_count']} | "
            f"part_files={part_idx} | output={out_path}"
        )

    except Exception as e:
        # Tangkap SEMUA exception tak terduga di sini supaya:
        # 1. Manifest tetap ditulis (job gagal, tapi jejaknya tercatat)
        # 2. Traceback lengkap muncul di log (bukan cuma exit code 1 polos)
        stats["status"] = f"error: {e}"
        try:
            write_manifest(year, month, stats)
        except Exception:
            pass
        print(f"[FAIL] Error tak terduga: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
          
