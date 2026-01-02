import pyarrow as pa
from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate primary dealer statistics output."""
    validate(table, {
        "columns": {
            "week_ending": "date",
            "series_name": "string",
            "series_code": "string",
            "asset_type": "string",
            "maturity_bucket": "string",
            "position_type": "string",
            "value_billions": "double",
        },
        "not_null": ["week_ending", "series_code", "asset_type"],
        "min_rows": 10,
    })

    asset_types = set(table.column("asset_type").to_pylist())
    valid_types = {"Treasury", "Agency", "MBS", "Corporate", "Other"}
    assert asset_types <= valid_types, f"Invalid asset types: {asset_types - valid_types}"

    print(f"  Validated {len(table):,} primary dealer statistics records")
