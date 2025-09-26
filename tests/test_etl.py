import pandas as pd
from etl.transform import normalize_types, dedupe_transactions
from etl.validate import split_valid_invalid


def test_normalize_types_handles_invalid_dates():
    df = pd.DataFrame([{"transaction_id": 1, "timestamp": "invalid-date"}])
    cleaned = normalize_types(df, default_date="2025-01-01")
    assert cleaned.loc[0, "timestamp"] == "2025-01-01"


def test_dedupe_transactions_removes_duplicates():
    df = pd.DataFrame([
        {"transaction_id": "1", "amount_original": 100},
        {"transaction_id": "1", "amount_original": 100}
    ])
    deduped = dedupe_transactions(df)
    assert len(deduped) == 1


def test_split_valid_invalid_separates_missing_transaction_id():
    df = pd.DataFrame(
        [{"transaction_id": None, "customer_id": 123, "amount_original": 50}])
    valid, errors = split_valid_invalid(df)
    assert len(valid) == 0
    assert len(errors) == 1
