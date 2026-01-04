"""Transform New York Fed reference rates data."""

import pyarrow as pa
from subsets_utils import load_raw_json, upload_data, publish
from utils import parse_date, parse_number
from .test import test

DATASET_ID = "nyf_reference_rates"

METADATA = {
    "id": DATASET_ID,
    "title": "New York Fed Reference Rates",
    "description": "Daily reference rates published by the New York Fed including SOFR, EFFR, OBFR, and other secured/unsecured overnight rates.",
    "column_descriptions": {
        "date": "Effective date of the rate",
        "rate_type": "Type of rate (SOFR, EFFR, OBFR, BGCR, TGCR)",
        "percentile_1": "1st percentile rate",
        "percentile_25": "25th percentile rate",
        "percentile_75": "75th percentile rate",
        "percentile_99": "99th percentile rate",
        "rate": "Published rate (percent)",
        "volume_billions": "Trading volume in billions USD",
        "target_rate_from": "Lower bound of target rate range",
        "target_rate_to": "Upper bound of target rate range",
    }
}

SCHEMA = pa.schema([
    pa.field("date", pa.date32(), nullable=False),
    pa.field("rate_type", pa.string(), nullable=False),
    pa.field("percentile_1", pa.float64()),
    pa.field("percentile_25", pa.float64()),
    pa.field("percentile_75", pa.float64()),
    pa.field("percentile_99", pa.float64()),
    pa.field("rate", pa.float64(), nullable=False),
    pa.field("volume_billions", pa.float64()),
    pa.field("target_rate_from", pa.float64()),
    pa.field("target_rate_to", pa.float64())
])


def process_rate_record(rate_data):
    date = parse_date(rate_data["effectiveDate"])
    rate_type = rate_data["type"]

    valid_types = {"EFFR", "OBFR", "SOFR", "BGCR", "TGCR"}
    if rate_type not in valid_types:
        return None

    return {
        "date": date,
        "rate_type": rate_type,
        "percentile_1": parse_number(rate_data.get("percentPercentile1")),
        "percentile_25": parse_number(rate_data.get("percentPercentile25")),
        "percentile_75": parse_number(rate_data.get("percentPercentile75")),
        "percentile_99": parse_number(rate_data.get("percentPercentile99")),
        "rate": parse_number(rate_data.get("percentRate")),
        "volume_billions": parse_number(rate_data.get("volumeInBillions")),
        "target_rate_from": parse_number(rate_data.get("targetRateFrom")),
        "target_rate_to": parse_number(rate_data.get("targetRateTo"))
    }


def run():
    """Transform reference rates raw data and upload."""
    raw_data = load_raw_json("reference_rates")

    records = []
    for rate_data in raw_data.get("unsecured", []):
        record = process_rate_record(rate_data)
        if record:
            records.append(record)

    for rate_data in raw_data.get("secured", []):
        record = process_rate_record(rate_data)
        if record:
            records.append(record)

    if not records:
        print("  No reference rate records found")
        return

    print(f"  Transformed {len(records):,} reference rate records")
    table = pa.Table.from_pylist(records, schema=SCHEMA)

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
