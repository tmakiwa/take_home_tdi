from pathlib import Path
import pandas as pd
import sqlite3
def write_outputs(valid_df: pd.DataFrame, errors_df: pd.DataFrame, suspicious_df: pd.DataFrame, out_dir: Path):
    out_dir = Path(out_dir)
    valid_df.to_csv(out_dir / "clean_transactions.csv", index=False)
    errors_df.to_csv(out_dir / "errors.csv", index=False)
    suspicious_df.to_csv(out_dir / "suspicious.csv", index=False)
    db_path = out_dir / "transactions.sqlite"
    with sqlite3.connect(db_path) as conn:
        valid_df.to_sql("transactions", conn, if_exists="replace", index=False)
