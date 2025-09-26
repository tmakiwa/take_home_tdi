import pandas as pd
from dateutil import parser

def normalize_types(df: pd.DataFrame, default_date=None) -> pd.DataFrame:
    df = df.copy()
    df["transaction_id"] = df["transaction_id"].astype(str)
    def parse_ts(x):
        if pd.isna(x) or str(x).strip()=="" or str(x).lower()=="none":
            return default_date
        try:
            return parser.parse(str(x)).date().isoformat()
        except Exception:
            return default_date
    df["timestamp"] = df["timestamp"].apply(parse_ts)
    if "customer_id" in df.columns:
        df["customer_id"] = df["customer_id"].astype(str)
    def to_float(x):
        try:
            return float(str(x).replace(",","").replace("$","").strip())
        except Exception:
            return None
    df["amount_original"] = df["amount_original"].apply(to_float)
    df["currency"] = df["currency"].astype(str).str.strip().str.upper()
    for opt in ["payment_method","status"]:
        if opt in df.columns:
            df[opt] = df[opt].astype(str)
    for col in ["source","amount_original","currency","timestamp"]:
        if col not in df.columns:
            df[col] = None
    return df

def dedupe_transactions(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(["transaction_id","source"]).drop_duplicates(subset=["transaction_id"], keep="first")

def flag_suspicious(df: pd.DataFrame, high_amount_usd=10000.0):
    if "amount_usd" in df.columns:
        susp = df[(df["amount_usd"] < 0) | (df["amount_usd"] > high_amount_usd)]
    else:
        susp = df[df["amount_original"] < 0]
    clean = df.drop(index=susp.index)
    return susp, clean
