"""Transform New York Fed primary dealer statistics data."""

from datetime import datetime
import pyarrow as pa
from subsets_utils import load_raw_json, upload_data, publish
from utils import parse_date
from .test import test

DATASET_ID = "nyf_primary_dealer_stats"

METADATA = {
    "id": DATASET_ID,
    "title": "New York Fed Primary Dealer Statistics",
    "description": "Weekly statistics on primary dealer positions and financing from the New York Fed. Covers Treasury, Agency, MBS, and corporate securities.",
    "column_descriptions": {
        "week_ending": "Week ending date",
        "series_name": "Name of the data series",
        "series_code": "Series code identifier",
        "asset_type": "Type of asset (Treasury, Agency, MBS, Corporate)",
        "maturity_bucket": "Maturity range for positions",
        "position_type": "Type of position (Net, Long, Short)",
        "value_billions": "Value in billions USD",
    }
}

SERIES_MAPPING = {
    "PDPOSGS": {"asset_type": "Treasury", "position_type": "Net", "maturity_bucket": None, "name": "Net Positions in U.S. Treasury Securities"},
    "PDPOSGSDS": {"asset_type": "Treasury", "position_type": "Net", "maturity_bucket": "3 years or less", "name": "Net Positions in U.S. Treasury Securities (≤3 years)"},
    "PDPOSGSDL": {"asset_type": "Treasury", "position_type": "Net", "maturity_bucket": "6+ years", "name": "Net Positions in U.S. Treasury Securities (>6 years)"},
    "PDPOSGSDI": {"asset_type": "Treasury", "position_type": "Net", "maturity_bucket": "3-6 years", "name": "Net Positions in U.S. Treasury Securities (3-6 years)"},
    "PDPOSFF": {"asset_type": "Agency", "position_type": "Net", "maturity_bucket": None, "name": "Net Positions in Agency Securities"},
    "PDPOSMBS": {"asset_type": "MBS", "position_type": "Net", "maturity_bucket": None, "name": "Net Positions in Agency MBS"},
    "PDPOSCD": {"asset_type": "Corporate", "position_type": "Long", "maturity_bucket": None, "name": "Positions in Corporate Debt"},
    "PDSOOS": {"asset_type": "Treasury", "position_type": "Short", "maturity_bucket": None, "name": "Securities Sold, Not Yet Purchased"},
    "PDFINRP": {"asset_type": "Treasury", "position_type": None, "maturity_bucket": None, "name": "Securities Purchased Under Repo"},
    "PDFINRR": {"asset_type": "Treasury", "position_type": None, "maturity_bucket": None, "name": "Securities Sold Under Repo"}
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


def parse_value(value):
    if value is None:
        return None
    try:
        return float(value) / 1000.0
    except (ValueError, TypeError):
        return None


def run():
    """Transform primary dealer stats raw data and upload."""
    raw_data = load_raw_json("primary_dealer_stats")

    series_data = raw_data.get("series_data", {})
    last_week_filter = raw_data.get("last_week_filter")
    last_week_date = datetime.strptime(last_week_filter, "%Y-%m-%d").date() if last_week_filter else None

    records = []
    for series_code, rows in series_data.items():
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

    print(f"  Transformed {len(records):,} primary dealer statistics records")
    table = pa.Table.from_pylist(records, schema=SCHEMA)

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
