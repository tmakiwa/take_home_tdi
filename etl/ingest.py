from pathlib import Path
import pandas as pd
import json
import xml.etree.ElementTree as ET

COMMON_COLUMNS = [
    "transaction_id","source","timestamp","customer_id",
    "amount_original","currency","payment_method","status"
]

POSSIBLE_MAP = {
    "transaction_id": ["transaction_id","id","txn_id","trans_id"],
    "timestamp": ["timestamp","ts","time","date","datetime","created_at","occurred_at","when"],
    "customer_id": ["customer_id","user_id","client_id","customer"],
    "amount_original": ["amount_original","amount","total","value","price"],
    "currency": ["currency","curr","ccy"],
    "payment_method": ["payment_method","payment","method"],
    "status": ["status","state"]
}

def _normalize_columns(df: pd.DataFrame, source: str) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    # Build a new frame with expected columns
    data = {col: None for col in COMMON_COLUMNS}
    for target, candidates in POSSIBLE_MAP.items():
        for c in candidates:
            if c in df.columns:
                data[target] = df[c]
                break

    out = pd.DataFrame(data)
    out["source"] = source
    # Keep extra columns if any (not required)
    for c in df.columns:
        if c not in out.columns:
            out[c] = df[c]
    return out

def load_csv(fp: Path, source: str) -> pd.DataFrame:
    df = pd.read_csv(fp)
    # Map "amount" -> amount_original if present
    if "amount" in df.columns and "amount_original" not in df.columns:
        df = df.rename(columns={"amount": "amount_original"})
    return _normalize_columns(df, source)

def load_json(fp: Path, source: str) -> pd.DataFrame:
    # Handle nested structure like:
    # id, customer.id, total.amount, total.currency, occurred_at, payment.method
    with open(fp, "r", encoding="utf-8") as f:
        raw = json.load(f)
    records = raw["data"] if isinstance(raw, dict) and "data" in raw else raw

    rows = []
    for rec in records:
        row = {}
        # top-level
        row["transaction_id"] = rec.get("id")
        row["source"] = rec.get("channel") or source
        # nested customer
        cust = rec.get("customer") or {}
        row["customer_id"] = cust.get("id")
        # nested totals
        tot = rec.get("total") or {}
        row["amount_original"] = tot.get("amount")
        row["currency"] = (tot.get("currency") or "").upper() if tot.get("currency") else None
        # time
        row["timestamp"] = rec.get("occurred_at")
        # payment
        pay = rec.get("payment") or {}
        row["payment_method"] = pay.get("method")
        # optional status if present
        row["status"] = rec.get("status")
        rows.append(row)

    df = pd.DataFrame(rows)
    return _normalize_columns(df, source)

def load_xml(fp: Path, source: str) -> pd.DataFrame:
    # Handle structure like:
    # <Transaction id="...">
    #   <Customer id="..."/>
    #   <Amount currency="GBP">15.75</Amount>
    #   <When>2025-08-31T06:25:00Z</When>
    #   <Payment method="bank_transfer"/>
    # </Transaction>
    tree = ET.parse(fp)
    root = tree.getroot()
    rows = []
    for tx in root.findall(".//Transaction"):
        row = {}
        row["transaction_id"] = tx.attrib.get("id")

        cust = tx.find("./Customer")
        if cust is not None:
            row["customer_id"] = cust.attrib.get("id")

        amt = tx.find("./Amount")
        if amt is not None:
            row["amount_original"] = (amt.text or "").strip() if amt.text else None
            row["currency"] = (amt.attrib.get("currency") or "").upper() if amt.attrib.get("currency") else None

        when = tx.find("./When")
        if when is not None:
            row["timestamp"] = (when.text or "").strip()

        pay = tx.find("./Payment")
        if pay is not None:
            row["payment_method"] = pay.attrib.get("method")

        row["source"] = source
        rows.append(row)

    df = pd.DataFrame(rows)
    return _normalize_columns(df, source)

def load_all_sources(input_dir: Path) -> pd.DataFrame:
    input_dir = Path(input_dir)
    frames = []

    # Known filenames from the assignment
    csv_fp = input_dir / "transactions_storeA.csv"
    json_fp = input_dir / "transactions_online.json"
    xml_fp = input_dir / "transactions_partner.xml"

    if csv_fp.exists():
        frames.append(load_csv(csv_fp, "storeA_csv"))
    if json_fp.exists():
        frames.append(load_json(json_fp, "online_json"))
    if xml_fp.exists():
        frames.append(load_xml(xml_fp, "partner_xml"))

    # If other files exist, try to infer by suffix
    for fp in input_dir.glob("*"):
        if fp.name in {csv_fp.name, json_fp.name, xml_fp.name}:
            continue
        if fp.suffix.lower() == ".csv":
            frames.append(load_csv(fp, fp.stem))
        elif fp.suffix.lower() == ".json":
            frames.append(load_json(fp, fp.stem))
        elif fp.suffix.lower() == ".xml":
            frames.append(load_xml(fp, fp.stem))

    if not frames:
        return pd.DataFrame(columns=COMMON_COLUMNS)
    return pd.concat(frames, ignore_index=True)
