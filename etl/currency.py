from __future__ import annotations
import json, os
import requests
import pandas as pd

API_BASE = "https://api.frankfurter.dev/v1"

def _load_cache(path: str) -> dict:
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def _save_cache(path: str, obj: dict) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, indent=2, sort_keys=True)
    except Exception:
        pass

def get_rate_to_usd(ccy: str, on_date: str|None, cache_path: str) -> float|None:
    if not ccy or ccy.upper()=="USD":
        return 1.0
    ccy = ccy.upper()
    cache = _load_cache(cache_path)
    key = f"{ccy}:{on_date or 'latest'}"
    if key in cache:
        return cache[key]
    url = f"{API_BASE}/{on_date}" if on_date else f"{API_BASE}/latest"
    params = {"from": ccy, "to": "USD"}
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    rate = data.get("rates", {}).get("USD")
    if rate is None:
        return None
    cache[key] = rate
    _save_cache(cache_path, cache)
    return rate

def convert_to_usd_for_df(df: pd.DataFrame, fx_cache_path: str) -> pd.DataFrame:
    df = df.copy()
    pairs = {(cur, ts) for cur, ts in zip(df["currency"], df["timestamp"])}
    rates = {}
    for cur, ts in pairs:
        rate = get_rate_to_usd(cur, ts, cache_path=fx_cache_path)
        rates[(cur, ts)] = rate if rate is not None else None
    def to_usd(row):
        r = rates.get((row["currency"], row["timestamp"]))
        if r in (None, 0):
            return None
        try:
            return float(row["amount_original"]) * float(r)
        except Exception:
            return None
    df["amount_usd"] = df.apply(to_usd, axis=1)
    return df
