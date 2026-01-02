import pyarrow as pa
from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate repo operations output."""
    validate(table, {
        "columns": {
            "operation_date": "date",
            "operation_id": "string",
            "operation_type": "string",
            "operation_method": "string",
            "settlement_date": "date",
            "maturity_date": "date",
            "term": "string",
            "term_calendar_days": "int",
            "settlement_type": "string",
            "security_type": "string",
            "amount_submitted": "double",
            "amount_accepted": "double",
            "total_amount_submitted": "double",
            "total_amount_accepted": "double",
            "participating_counterparties": "int",
            "accepted_counterparties": "int",
            "offering_rate": "double",
            "award_rate": "double",
            "weighted_average_rate": "double",
            "minimum_bid_rate": "double",
            "maximum_bid_rate": "double",
            "release_time": "string",
            "close_time": "string",
        },
        "not_null": ["operation_date", "operation_id"],
        "min_rows": 10,
    })

    print(f"  Validated {len(table):,} repo operation records")
