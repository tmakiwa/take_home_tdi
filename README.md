# Transaction Data Integration (ETL)

This project is a take-home assignment that demonstrates the design and implementation of an **ETL pipeline in Python**.  
The goal is to ingest transaction data from multiple sources (**CSV, JSON, XML**), validate and clean it, standardize into a consistent schema, convert amounts to USD, and store results for analysis.

---

## Project Structure

```
txn_integration/
├── main.py                 # Orchestrator script
├── etl/
│   ├── ingest.py           # Load and unify CSV, JSON, XML
│   ├── transform.py        # Normalize, dedupe, suspicious flags
│   ├── validate.py         # Required fields & invalid record handling
│   ├── currency.py         # Currency conversion (Frankfurter API)
│   └── storage.py          # Save outputs to CSV + SQLite
├── tests/
│   └── test_transform.py   # Sample pytest tests
├── requirements.txt        # Python dependencies
└── README.md               # Documentation
```

---

## Setup & Run

1. Clone the repo or unzip the project:

   ```bash
   git clone https://github.com/<your-username>/transaction-data-integration.git
   cd transaction-data-integration
   ```

2. Create and activate a virtual environment:

   ```bash
   python -m venv .venv
   source .venv/bin/activate   # Mac/Linux
   .venv\Scripts\activate      # Windows
   ```

3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. Place input files into a `data/` folder:

   - `transactions_storeA.csv`
   - `transactions_online.json`
   - `transactions_partner.xml`

5. Run the pipeline:

   ```bash
   python main.py --input-dir data --out-dir out
   ```

6. Optional: provide a fallback date for missing timestamps:

   ```bash
   python main.py --input-dir data --out-dir out --default-date 2025-01-01
   ```

7. Run tests:
   ```bash
   pytest -q
   ```

---

## Target Schema

| Column            | Description                                        |
| ----------------- | -------------------------------------------------- |
| `transaction_id`  | Unique identifier for the transaction              |
| `source`          | Source system/file (e.g., storeA, online, partner) |
| `timestamp`       | Transaction timestamp (ISO format)                 |
| `customer_id`     | Unique customer identifier                         |
| `amount_original` | Original transaction amount                        |
| `currency`        | Original currency                                  |
| `amount_usd`      | Converted amount in USD (via Frankfurter API)      |
| `payment_method`  | Payment channel/method                             |
| `status`          | Transaction status (if provided)                   |

---

## Outputs

- `clean_transactions.csv` → standardized, validated dataset (with `amount_usd`)
- `errors.csv` → invalid rows (with reasons)
- `suspicious.csv` → flagged negative or unusually large amounts
- `transactions.sqlite` → SQLite DB with `transactions` table

---

## Key Features

- **Ingestion** → CSV (pandas), JSON (json), XML (ElementTree)
- **Normalization** → consistent schema for all sources
- **Validation** → required field checks, invalid rows quarantined
- **Deduplication** → drops duplicate `transaction_id`s
- **Currency Conversion** → Frankfurter API (historical by date, cached)
- **Suspicious Detection** → negative & abnormally large amounts
- **Storage** → outputs to CSV + SQLite
- **Testing** → pytest unit tests for transform/validation logic

---

## Assumptions & Design Decisions

- Schema-first design keeps the pipeline simple and predictable.
- Invalid rows are logged to `errors.csv` instead of breaking the run.
- Frankfurter API provides reliable FX rates (historical when date is known).
- SQLite chosen for portability; easily replaced by MySQL/Postgres in production.
- Suspicious rules are intentionally simple (negative / huge amounts).
- Modular file structure mirrors real ETL pipelines and is easy to extend.

---

## ETL Flow

```
CSV / JSON / XML
       ↓
   Ingest (etl/ingest.py)
       ↓
   Normalize & Clean (etl/transform.py)
       ↓
   Validate & Split (etl/validate.py)
       ↓
   Deduplicate
       ↓
   Currency Conversion (etl/currency.py)
       ↓
   Flag Suspicious
       ↓
   Store → clean_transactions.csv / errors.csv / suspicious.csv / transactions.sqlite
```

---

## Disclaimer

This solution is for **demonstration purposes only**. Deduction rates, suspicious thresholds, and some assumptions are illustrative. In production, these would be refined based on business rules, compliance, and performance requirements.
