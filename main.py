#!/usr/bin/env python3
import argparse
from pathlib import Path
from etl.ingest import load_all_sources
from etl.transform import normalize_types, dedupe_transactions, flag_suspicious
from etl.validate import split_valid_invalid
from etl.currency import convert_to_usd_for_df
from etl.storage import write_outputs


def parse_args():
    p = argparse.ArgumentParser(
        description="Transaction Data Integration (CSV/JSON/XML → standardized + USD)")
    p.add_argument("--input-dir", required=True,
                   help="Directory containing input files")
    p.add_argument("--out-dir", required=True,
                   help="Directory to write outputs")
    p.add_argument(
        "--default-date", help="YYYY-MM-DD: used if a record lacks a parsable timestamp")
    p.add_argument("--fx-cache", default=".fx_cache.json",
                   help="Path to FX cache file")
    return p.parse_args()


def main():
    args = parse_args()
    in_dir = Path(args.input_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = load_all_sources(in_dir)
    if df.empty:
        print("No records loaded. Check your data directory and filenames.")
        return 0

    df = normalize_types(df, default_date=args.default_date)
    valid_df, errors_df = split_valid_invalid(df)
    valid_df = dedupe_transactions(valid_df)
    valid_df = convert_to_usd_for_df(valid_df, fx_cache_path=args.fx_cache)
    suspicious_df, valid_df = flag_suspicious(valid_df)
    write_outputs(valid_df, errors_df, suspicious_df, out_dir)
    print(f"Done. Wrote outputs to: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
