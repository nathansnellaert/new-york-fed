"""NY Fed Primary Dealer Statistics - ingest and transform.

Fetches and transforms weekly primary dealer position data.
Source: https://markets.newyorkfed.org/
"""

import asyncio
import csv
from datetime import datetime
from io import StringIO
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import pyarrow as pa
from subsets_utils import save_raw_json, load_raw_json, load_state, save_state, merge, publish, validate

BASE_URL = "https://markets.newyorkfed.org/api"
DATASET_ID = "nyf_primary_dealer_stats"

SERIES_CODES = [
    "PDPOSGS", "PDPOSGSDS", "PDPOSGSDL", "PDPOSGSDI",
    "PDPOSFF", "PDPOSMBS", "PDPOSCD", "PDSOOS",
    "PDFINRP", "PDFINRR",
]

SERIES_MAPPING = {
    "PDPOSGS": {"asset_type": "Treasury", "position_type": "Net", "maturity_bucket": None, "name": "Net Positions in U.S. Treasury Securities"},
    "PDPOSGSDS": {"asset_type": "Treasury", "position_type": "Net", "maturity_bucket": "3 years or less", "name": "Net Positions (<=3 years)"},
    "PDPOSGSDL": {"asset_type": "Treasury", "position_type": "Net", "maturity_bucket": "6+ years", "name": "Net Positions (>6 years)"},
    "PDPOSGSDI": {"asset_type": "Treasury", "position_type": "Net", "maturity_bucket": "3-6 years", "name": "Net Positions (3-6 years)"},
    "PDPOSFF": {"asset_type": "Agency", "position_type": "Net", "maturity_bucket": None, "name": "Net Positions in Agency Securities"},
    "PDPOSMBS": {"asset_type": "MBS", "position_type": "Net", "maturity_bucket": None, "name": "Net Positions in Agency MBS"},
    "PDPOSCD": {"asset_type": "Corporate", "position_type": "Long", "maturity_bucket": None, "name": "Positions in Corporate Debt"},
    "PDSOOS": {"asset_type": "Treasury", "position_type": "Short", "maturity_bucket": None, "name": "Securities Sold, Not Yet Purchased"},
    "PDFINRP": {"asset_type": "Treasury", "position_type": None, "maturity_bucket": None, "name": "Securities Purchased Under Repo"},
    "PDFINRR": {"asset_type": "Treasury", "position_type": None, "maturity_bucket": None, "name": "Securities Sold Under Repo"}
}

METADATA = {
    "id": DATASET_ID,
    "title": "NY Fed Primary Dealer Statistics",
    "description": "Weekly primary dealer position and financing data from the New York Fed, covering Treasury, Agency, MBS, and corporate securities.",
    "column_descriptions": {
        "week_ending": "Week-ending date of the reported position",
        "series_name": "Descriptive name of the statistical series",
        "series_code": "NY Fed series code identifier (e.g., PDPOSGS, PDFINRP)",
        "asset_type": "Asset class (Treasury, Agency, MBS, Corporate)",
        "maturity_bucket": "Maturity range bucket (e.g., 3 years or less, 3-6 years, 6+ years)",
        "position_type": "Type of position (Net, Long, Short)",
        "value_billions": "Position value in billions of dollars",
    },
}

SCHEMA = pa.schema([
    pa.field("week_ending", pa.date32()),
    pa.field("series_name", pa.string()),
    pa.field("series_code", pa.string()),
    pa.field("asset_type", pa.string()),
    pa.field("maturity_bucket", pa.string()),
    pa.field("position_type", pa.string()),
    pa.field("value_billions", pa.float64())
])


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(httpx.HTTPError)
)
async def fetch_series_csv(client, series_code):
    url = f"{BASE_URL}/pd/latest/{series_code}.csv"
    response = await client.get(url, timeout=30)
    response.raise_for_status()
    return response.text


async def fetch_all_series_async():
    async with httpx.AsyncClient() as client:
        tasks = []
        for series_code in SERIES_CODES:
            task = fetch_series_csv(client, series_code)
            tasks.append((series_code, task))

        results = {}
        for series_code, task in tasks:
            try:
                csv_content = await task
                reader = csv.DictReader(StringIO(csv_content))
                results[series_code] = list(reader)
            except Exception as e:
                print(f"    Error fetching {series_code}: {e}")
                results[series_code] = []

        return results


def parse_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def parse_value(value):
    if value is None:
        return None
    try:
        return float(value) / 1000.0
    except (ValueError, TypeError):
        return None


def test(table: pa.Table) -> None:
    validate(table, {
        "columns": {
            "week_ending": "date",
            "series_code": "string",
            "asset_type": "string",
        },
        "not_null": ["week_ending", "series_code", "asset_type"],
        "min_rows": 1,
    })

    asset_types = set(table.column("asset_type").to_pylist())
    valid_types = {"Treasury", "Agency", "MBS", "Corporate", "Other"}
    assert asset_types <= valid_types, f"Invalid asset types: {asset_types - valid_types}"

    print(f"  Validated {len(table):,} primary dealer records")


def run():
    """Ingest and transform primary dealer statistics."""
    # Ingest
    print("Fetching NY Fed primary dealer statistics...")
    state = load_state("primary_dealer_stats")
    last_week = state.get("last_week")
    last_week_date = datetime.strptime(last_week, "%Y-%m-%d").date() if last_week else None

    raw_data = asyncio.run(fetch_all_series_async())

    max_date = None
    total_records = 0
    for series_code, rows in raw_data.items():
        total_records += len(rows)
        for row in rows:
            if 'As Of Date' in row:
                record_date = datetime.strptime(row['As Of Date'], "%Y-%m-%d").date()
                if max_date is None or record_date > max_date:
                    max_date = record_date

    save_raw_json({
        "series_data": raw_data,
        "last_week_filter": last_week
    }, "primary_dealer_stats")

    print(f"  Fetched {total_records} records across {len(SERIES_CODES)} series")

    # Transform
    print("Transforming primary dealer statistics...")
    records = []
    for series_code, rows in raw_data.items():
        series_info = SERIES_MAPPING.get(series_code, {})

        for row in rows:
            if 'As Of Date' not in row or 'Value' not in row:
                continue

            week_ending = parse_date(row['As Of Date'])
            if last_week_date and week_ending <= last_week_date:
                continue

            records.append({
                "week_ending": week_ending,
                "series_name": series_info.get("name", series_code),
                "series_code": series_code,
                "asset_type": series_info.get("asset_type", "Other"),
                "maturity_bucket": series_info.get("maturity_bucket"),
                "position_type": series_info.get("position_type"),
                "value_billions": parse_value(row['Value'])
            })

    if not records:
        print("  No primary dealer statistics records found")
        return

    print(f"  Transformed {len(records):,} records")
    table = pa.Table.from_pylist(records, schema=SCHEMA)

    test(table)
    merge(table, DATASET_ID, key=["week_ending", "series_code"])
    publish(DATASET_ID, METADATA)

    if max_date and (not last_week_date or max_date > last_week_date):
        save_state("primary_dealer_stats", {"last_week": max_date.strftime("%Y-%m-%d")})


NODES = {
    run: [],
}

if __name__ == "__main__":
    run()
