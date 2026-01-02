import pyarrow as pa
from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate Treasury operations output."""
    validate(table, {
        "columns": {
            "operation_date": "date",
            "operation_id": "string",
            "operation_type": "string",
            "operation_direction": "string",
            "settlement_date": "date",
            "cusip": "string",
            "security_description": "string",
            "maturity_date_start": "date",
            "maturity_date_end": "date",
            "auction_method": "string",
            "par_amount_submitted": "double",
            "par_amount_accepted": "double",
            "weighted_avg_price": "double",
            "least_favorable_price": "double",
            "release_time": "string",
            "close_time": "string",
        },
        "not_null": ["operation_date", "operation_id", "par_amount_accepted"],
        "min_rows": 10,
    })

    print(f"  Validated {len(table):,} Treasury operation records")
