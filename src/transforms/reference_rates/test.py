import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_in_range


def test(table: pa.Table) -> None:
    """Validate reference rates output."""
    validate(table, {
        "columns": {
            "date": "date",
            "rate_type": "string",
            "percentile_1": "double",
            "percentile_25": "double",
            "percentile_75": "double",
            "percentile_99": "double",
            "rate": "double",
            "volume_billions": "double",
            "target_rate_from": "double",
            "target_rate_to": "double",
        },
        "not_null": ["date", "rate_type", "rate"],
        "min_rows": 10,
    })

    assert_in_range(table, "rate", -1, 20)

    rate_types = set(table.column("rate_type").to_pylist())
    valid_types = {"SOFR", "EFFR", "OBFR", "BGCR", "TGCR"}
    assert rate_types <= valid_types, f"Invalid rate types: {rate_types - valid_types}"

    print(f"  Validated {len(table):,} reference rate records")
