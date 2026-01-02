import pyarrow as pa
from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate SOMA holdings output."""
    validate(table, {
        "columns": {
            "as_of_date": "date",
            "security_type": "string",
            "cusip": "string",
            "security_description": "string",
            "maturity_date": "date",
            "issuer": "string",
            "coupon_rate": "double",
            "par_value": "double",
            "percent_outstanding": "double",
            "change_from_prior_week": "double",
            "change_from_prior_year": "double",
        },
        "not_null": ["as_of_date", "security_type", "cusip"],
        "min_rows": 10,
    })

    security_types = set(table.column("security_type").to_pylist())
    assert len(security_types) >= 1, "Should have at least one security type"

    print(f"  Validated {len(table):,} SOMA holdings records")
