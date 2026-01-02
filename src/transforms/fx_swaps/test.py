import pyarrow as pa
from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate FX swaps output."""
    validate(table, {
        "columns": {
            "trade_date": "date",
            "settlement_date": "date",
            "maturity_date": "date",
            "operation_type": "string",
            "counterparty": "string",
            "currency": "string",
            "term_days": "int",
            "amount": "double",
            "interest_rate": "double",
            "is_small_value": "bool",
            "last_updated": "timestamp",
        },
        "not_null": ["trade_date", "currency"],
        "min_rows": 5,
    })

    print(f"  Validated {len(table):,} FX swap records")
