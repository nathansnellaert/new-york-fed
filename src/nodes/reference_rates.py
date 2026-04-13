"""NY Fed Reference Rates - ingest and transform.

Fetches and transforms SOFR, EFFR, OBFR, and other reference rates.
Source: https://markets.newyorkfed.org/
"""

from datetime import datetime, timedelta
import pyarrow as pa
from subsets_utils import get, save_raw_json, load_raw_json, load_state, save_state, merge, publish, validate
from subsets_utils.testing import assert_in_range

BASE_URL = "https://markets.newyorkfed.org/api"
DATASET_ID = "nyf_reference_rates"

METADATA = {
    "id": DATASET_ID,
    "title": "NY Fed Reference Rates",
    "description": "Daily reference interest rates (SOFR, EFFR, OBFR, BGCR, TGCR) published by the New York Fed.",
    "column_descriptions": {
        "date": "Effective date of the rate observation",
        "rate_type": "Reference rate type (SOFR, EFFR, OBFR, BGCR, TGCR)",
        "percentile_1": "1st percentile rate",
        "percentile_25": "25th percentile rate",
        "percentile_75": "75th percentile rate",
        "percentile_99": "99th percentile rate",
        "rate": "Published reference rate (percent)",
        "volume_billions": "Transaction volume in billions of dollars",
        "target_rate_from": "Lower bound of the federal funds target rate range",
        "target_rate_to": "Upper bound of the federal funds target rate range",
    },
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


def fetch_rate_data(endpoint):
    """Fetch rate data from NY Fed API"""
    url = f"{BASE_URL}/{endpoint}"
    response = get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def parse_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def parse_number(value):
    if value is None or value == "NA":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def test(table: pa.Table) -> None:
    """Validate reference rates output."""
    validate(table, {
        "columns": {
            "date": "date",
            "rate_type": "string",
            "rate": "double",
        },
        "not_null": ["date", "rate_type", "rate"],
        "min_rows": 1,
    })

    assert_in_range(table, "rate", -1, 20)

    rate_types = set(table.column("rate_type").to_pylist())
    valid_types = {"SOFR", "EFFR", "OBFR", "BGCR", "TGCR"}
    assert rate_types <= valid_types, f"Invalid rate types: {rate_types - valid_types}"

    print(f"  Validated {len(table):,} reference rate records")


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
    """Ingest and transform reference rates."""
    # Ingest
    print("Fetching NY Fed reference rates...")
    state = load_state("reference_rates")
    last_date = state.get("last_date")

    if last_date:
        start_date = datetime.strptime(last_date, "%Y-%m-%d").date() + timedelta(days=1)
    else:
        start_date = datetime(2018, 4, 3).date()

    end_date = datetime.now().date() - timedelta(days=1)

    if start_date > end_date:
        print("  No new reference rates data to fetch")
        return

    print(f"  Fetching from {start_date} to {end_date}")

    all_unsecured = []
    all_secured = []

    current_start = start_date
    while current_start <= end_date:
        chunk_end = min(current_start + timedelta(days=89), end_date)

        params = {
            "startDate": current_start.strftime("%Y-%m-%d"),
            "endDate": chunk_end.strftime("%Y-%m-%d")
        }

        print(f"    Fetching {params['startDate']} to {params['endDate']}")

        unsecured_data = fetch_rate_data(f"rates/all/search.json?startDate={params['startDate']}&endDate={params['endDate']}")
        if unsecured_data and "refRates" in unsecured_data:
            all_unsecured.extend(unsecured_data["refRates"])

        secured_data = fetch_rate_data(f"rates/secured/all/search.json?startDate={params['startDate']}&endDate={params['endDate']}")
        if secured_data and "refRates" in secured_data:
            all_secured.extend(secured_data["refRates"])

        current_start = chunk_end + timedelta(days=1)

    raw_data = {
        "unsecured": all_unsecured,
        "secured": all_secured,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d")
    }
    save_raw_json(raw_data, "reference_rates")

    print(f"  Fetched {len(all_unsecured)} unsecured + {len(all_secured)} secured records")

    # Transform
    print("Transforming reference rates...")
    seen = set()
    records = []
    for rate_data in raw_data.get("unsecured", []) + raw_data.get("secured", []):
        record = process_rate_record(rate_data)
        if record:
            key = (record["rate_type"], record["date"])
            if key not in seen:
                seen.add(key)
                records.append(record)

    if not records:
        print("  No reference rate records found")
        return

    print(f"  Transformed {len(records):,} records")
    table = pa.Table.from_pylist(records, schema=SCHEMA)

    test(table)
    merge(table, DATASET_ID, key=["rate_type", "date"])
    publish(DATASET_ID, METADATA)

    if all_unsecured or all_secured:
        save_state("reference_rates", {"last_date": end_date.strftime("%Y-%m-%d")})


NODES = {
    run: [],
}

if __name__ == "__main__":
    run()
