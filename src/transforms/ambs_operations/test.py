import pyarrow as pa
from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate AMBS operations output."""
    validate(table, {
        "columns": {
            "operation_date": "date",
            "operation_id": "string",
            "operation_type": "string",
            "operation_direction": "string",
            "settlement_date": "date",
            "security_description": "string",
            "class_type": "string",
            "method": "string",
            "amount_submitted_par": "double",
            "amount_accepted_par": "double",
            "release_time": "string",
            "close_time": "string",
            "inclusion_flag": "string",
        },
        "not_null": ["operation_date", "operation_id"],
        "min_rows": 10,
    })

    print(f"  Validated {len(table):,} AMBS operation records")
