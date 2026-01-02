
import asyncio
import csv
from datetime import datetime
from io import StringIO
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from subsets_utils import save_raw_json, load_state, save_state

BASE_URL = "https://markets.newyorkfed.org/api"

# Key series to track
SERIES_CODES = [
    "PDPOSGS",    # Net Positions in U.S. Treasury Securities
    "PDPOSGSDS",  # Net Positions in U.S. Treasury Securities (≤3 years)
    "PDPOSGSDL",  # Net Positions in U.S. Treasury Securities (>6 years)
    "PDPOSGSDI",  # Net Positions in U.S. Treasury Securities (3-6 years)
    "PDPOSFF",    # Net Positions in Agency Securities
    "PDPOSMBS",   # Net Positions in Agency MBS
    "PDPOSCD",    # Positions in Corporate Debt
    "PDSOOS",     # Securities Sold, Not Yet Purchased
    "PDFINRP",    # Securities Purchased Under Repo
    "PDFINRR",    # Securities Sold Under Repo
]


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(httpx.HTTPError)
)
async def fetch_series_csv(client, series_code):
    """Fetch data for a specific series (CSV format)"""
    url = f"{BASE_URL}/pd/latest/{series_code}.csv"
    response = await client.get(url, timeout=30)
    response.raise_for_status()
    return response.text


async def fetch_all_series_async():
    """Fetch all series data concurrently"""
    async with httpx.AsyncClient() as client:
        tasks = []
        for series_code in SERIES_CODES:
            task = fetch_series_csv(client, series_code)
            tasks.append((series_code, task))

        results = {}
        for series_code, task in tasks:
            try:
                csv_content = await task
                # Parse CSV to list of dicts
                reader = csv.DictReader(StringIO(csv_content))
                results[series_code] = list(reader)
            except Exception as e:
                print(f"Error fetching {series_code}: {e}")
                results[series_code] = []

        return results


def run():
    """Fetch primary dealer statistics and save raw JSON"""
    state = load_state("primary_dealer_stats")
    last_week = state.get("last_week")
    last_week_date = datetime.strptime(last_week, "%Y-%m-%d").date() if last_week else None

    raw_data = asyncio.run(fetch_all_series_async())

    # Find the latest date across all series
    max_date = None
    total_records = 0
    for series_code, records in raw_data.items():
        total_records += len(records)
        for record in records:
            if 'As Of Date' in record:
                record_date = datetime.strptime(record['As Of Date'], "%Y-%m-%d").date()
                if max_date is None or record_date > max_date:
                    max_date = record_date

    # Save raw data
    save_raw_json({
        "series_data": raw_data,
        "last_week_filter": last_week
    }, "primary_dealer_stats")

    # Update state if we have new data
    if max_date and (not last_week_date or max_date > last_week_date):
        save_state("primary_dealer_stats", {"last_week": max_date.strftime("%Y-%m-%d")})

    print(f"Fetched {total_records} primary dealer records across {len(SERIES_CODES)} series")
