# etl/transform.py
import pandas as pd
from dateutil import parser

REQUIRED_BASE = ["transaction_id", "timestamp", "customer_id",
                 "amount_original", "currency", "source"]
OPTIONAL = ["payment_method", "status"]


def normalize_types(df: pd.DataFrame, default_date=None) -> pd.DataFrame:
    df = df.copy()

    # Ensure required columns exist so downstream ops don't KeyError
    for col in REQUIRED_BASE + OPTIONAL:
        if col not in df.columns:
            df[col] = None

    # transaction_id as string
    df["transaction_id"] = df["transaction_id"].astype(str)

    # timestamp → ISO date, fallback to default_date if invalid/missing
    def parse_ts(x):
        if pd.isna(x) or str(x).strip() == "" or str(x).lower() == "none":
            return default_date
        try:
            return parser.parse(str(x)).date().isoformat()
        except Exception:
            return default_date
    df["timestamp"] = df["timestamp"].apply(parse_ts)

    # customer_id as string (if present)
    if "customer_id" in df.columns:
        df["customer_id"] = df["customer_id"].astype(str)

    # amount_original → float (strip commas/currency symbols)
    def to_float(x):
        try:
            return float(str(x).replace(",", "").replace("$", "").strip())
        except Exception:
            return None
    df["amount_original"] = df["amount_original"].apply(to_float)

    # currency → uppercase 3-letter where possible
    df["currency"] = df["currency"].astype(str).str.strip().str.upper()

    # string-ify optional fields if present
    for opt in ["payment_method", "status", "source"]:
        if opt in df.columns:
            df[opt] = df[opt].astype(str)

    return df


def dedupe_transactions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # If 'source' is missing (or all None), sort only by transaction_id
    sort_cols = ["transaction_id"]
    if "source" in df.columns:
        sort_cols.append("source")
    return df.sort_values(sort_cols).drop_duplicates(subset=["transaction_id"], keep="first")


def flag_suspicious(df: pd.DataFrame, high_amount_usd=10000.0):
    df = df.copy()
    if "amount_usd" in df.columns:
        susp = df[(df["amount_usd"] < 0) | (
            df["amount_usd"] > high_amount_usd)]
    else:
        susp = df[df["amount_original"] < 0]
    clean = df.drop(index=susp.index)
    return susp, clean
