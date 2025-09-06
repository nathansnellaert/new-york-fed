import asyncio
from datetime import datetime
import httpx
import pyarrow as pa
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from utils.io import load_state, save_state

BASE_URL = "https://markets.newyorkfed.org/api"

# Key series to track
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

schema = pa.schema([
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
async def fetch_pd_data(client, endpoint):
    """Fetch Primary Dealer data from NY Fed API"""
    url = f"{BASE_URL}/{endpoint}"
    response = await client.get(url, timeout=30)
    response.raise_for_status()
    return response.json()

def parse_date(date_str):
    """Parse date string to date object"""
    return datetime.strptime(date_str, "%Y-%m-%d").date()

def parse_value(value):
    """Parse numeric value, handling None"""
    if value is None:
        return None
    try:
        # Values are in millions, convert to billions
        return float(value) / 1000.0
    except (ValueError, TypeError):
        return None

async def fetch_series_data(client, series_code, start_date=None):
    """Fetch data for a specific series"""
    # Use CSV endpoint - only latest endpoint works
    endpoint = f"pd/latest/{series_code}.csv"
    
    try:
        url = f"{BASE_URL}/{endpoint}"
        response = await client.get(url, timeout=30)
        response.raise_for_status()
        
        # Parse CSV response
        import csv
        from io import StringIO
        
        csv_content = response.text
        reader = csv.DictReader(StringIO(csv_content))
        
        records = []
        series_info = SERIES_MAPPING.get(series_code, {})
        
        for row in reader:
            if 'As Of Date' in row and 'Value' in row:
                record = {
                    "week_ending": parse_date(row['As Of Date']),
                    "series_name": series_info.get("name", series_code),
                    "series_code": series_code,
                    "asset_type": series_info.get("asset_type", "Other"),
                    "maturity_bucket": series_info.get("maturity_bucket"),
                    "position_type": series_info.get("position_type"),
                    "value_billions": parse_value(row['Value'])
                }
                records.append(record)
        
        return records
    except Exception as e:
        print(f"Error fetching {series_code}: {e}")
        return []

async def process_primary_dealer_stats_async():
    """Process primary dealer statistics data"""
    state = load_state("primary_dealer_stats")
    last_week = state.get("last_week")
    last_week_date = datetime.strptime(last_week, "%Y-%m-%d").date() if last_week else None
    
    all_records = []
    
    async with httpx.AsyncClient() as client:
        # Fetch data for all series concurrently (only latest available)
        tasks = []
        for series_code in SERIES_MAPPING.keys():
            task = fetch_series_data(client, series_code)
            tasks.append(task)
        
        series_results = await asyncio.gather(*tasks)
        
        # Combine all records and filter for new data only
        for records in series_results:
            if last_week_date:
                # Only include records newer than last processed week
                new_records = [r for r in records if r["week_ending"] > last_week_date]
                all_records.extend(new_records)
            else:
                all_records.extend(records)
    
    if not all_records:
        print("No new primary dealer data to fetch")
        return pa.Table.from_pylist([], schema=schema)
    
    # Update state with the latest week
    max_date = max(r["week_ending"] for r in all_records)
    state["last_week"] = max_date.strftime("%Y-%m-%d")
    save_state("primary_dealer_stats", state)
    
    print(f"Fetched {len(all_records)} primary dealer statistics records")
    return pa.Table.from_pylist(all_records, schema=schema)

def process_primary_dealer_stats():
    """Synchronous wrapper for async processing"""
    return asyncio.run(process_primary_dealer_stats_async())