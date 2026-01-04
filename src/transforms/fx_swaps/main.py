"""Transform New York Fed FX swaps data."""

from datetime import datetime
import pyarrow as pa
from subsets_utils import load_raw_json, upload_data, publish
from .test import test

DATASET_ID = "nyf_fx_swaps"

METADATA = {
    "id": DATASET_ID,
    "title": "New York Fed Foreign Exchange Swaps",
    "description": "Foreign exchange swap operations conducted by the New York Fed, including central bank liquidity swap arrangements.",
    "column_descriptions": {
        "trade_date": "Date the swap was traded",
        "settlement_date": "Settlement date of the swap",
        "maturity_date": "Maturity date of the swap",
        "operation_type": "Type of FX swap operation",
        "counterparty": "Counterparty central bank",
        "currency": "Currency of the swap",
        "term_days": "Term of the swap in days",
        "amount": "Amount of the swap",
        "interest_rate": "Interest rate on the swap",
        "is_small_value": "Flag indicating small value operation",
        "last_updated": "Last update timestamp",
    }
}

SCHEMA = pa.schema([
    pa.field("trade_date", pa.date32()),
    pa.field("settlement_date", pa.date32()),
    pa.field("maturity_date", pa.date32()),
    pa.field("operation_type", pa.string()),
    pa.field("counterparty", pa.string()),
    pa.field("currency", pa.string()),
    pa.field("term_days", pa.int32()),
    pa.field("amount", pa.float64()),
    pa.field("interest_rate", pa.float64()),
    pa.field("is_small_value", pa.bool_()),
    pa.field("last_updated", pa.timestamp('s'))
])


def parse_date(date_str):
    if not date_str:
        return None
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def parse_timestamp(timestamp_str):
    if not timestamp_str:
        return None
    try:
        return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M")
        except ValueError:
            return None


def parse_bool(value):
    if value is None or value == "":
        return None
    return str(value).lower() in ['true', '1', 'yes']


def run():
    """Transform FX swaps raw data and upload."""
    raw_data = load_raw_json("fx_swaps")

    operations = raw_data.get("operations", [])
    records = []

    for swap in operations:
        records.append({
            "trade_date": parse_date(swap.get("tradeDate")),
            "settlement_date": parse_date(swap.get("settlementDate")),
            "maturity_date": parse_date(swap.get("maturityDate")),
            "operation_type": swap.get("operationType"),
            "counterparty": swap.get("counterparty"),
            "currency": swap.get("currency"),
            "term_days": int(swap.get("termInDays", 0)) if swap.get("termInDays") else None,
            "amount": float(swap.get("amount", 0)) if swap.get("amount") else None,
            "interest_rate": float(swap.get("interestRate", 0)) if swap.get("interestRate") else None,
            "is_small_value": parse_bool(swap.get("isSmallValue")),
            "last_updated": parse_timestamp(swap.get("lastUpdated"))
        })

    if not records:
        print("  No FX swap records found")
        return

    print(f"  Transformed {len(records):,} FX swap records")
    table = pa.Table.from_pylist(records, schema=SCHEMA)

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
