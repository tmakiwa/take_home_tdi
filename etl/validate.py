import pandas as pd
REQUIRED = ["transaction_id","timestamp","customer_id","amount_original","currency","source"]
def _reason(row):
    missing = [c for c in REQUIRED if pd.isna(row.get(c)) or str(row.get(c)).strip()=="" or row.get(c) is None]
    reasons = []
    if missing:
        reasons.append(f"Missing: {','.join(missing)}")
    try:
        amt = float(row.get("amount_original"))
        if abs(amt) > 1e9:
            reasons.append("Amount too large")
    except Exception:
        reasons.append("Amount not numeric")
    cur = str(row.get("currency") or "").upper()
    if len(cur) != 3:
        reasons.append("Currency not 3-letter code")
    return "; ".join(reasons)
def split_valid_invalid(df: pd.DataFrame):
    df = df.copy()
    issues = df.apply(_reason, axis=1)
    invalid_mask = issues.str.len() > 0
    errors = df[invalid_mask].copy()
    errors["error_reason"] = issues[invalid_mask]
    valid = df[~invalid_mask].copy()
    return valid, errors
