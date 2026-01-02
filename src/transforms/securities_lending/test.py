import pyarrow as pa
from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate securities lending output."""
    validate(table, {
        "columns": {
            "operation_date": "date",
            "operation_id": "string",
            "settlement_date": "date",
            "maturity_date": "date",
            "cusip": "string",
            "security_description": "string",
            "par_amount_submitted": "double",
            "par_amount_accepted": "double",
            "weighted_average_rate": "double",
            "soma_holdings": "double",
            "theoretical_available": "double",
            "actual_available": "double",
            "outstanding_loans": "double",
            "release_time": "string",
            "close_time": "string",
        },
        "not_null": ["operation_date", "operation_id", "cusip"],
        "min_rows": 10,
    })

    print(f"  Validated {len(table):,} Securities Lending records")
