import asyncio
from datetime import datetime, timedelta
import httpx
import pyarrow as pa
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from utils.io import load_state, save_state

BASE_URL = "https://markets.newyorkfed.org/api"

schema = pa.schema([
    pa.field("trade_date", pa.date32()),
    pa.field("settlement_date", pa.date32()),
    pa.field("maturity_date", pa.date32()),
    pa.field("operation_type", pa.string()),
    pa.field("counterparty", pa.string()),
    pa.field("currency", pa.string()),
    pa.field("term_days", pa.int32()),
    pa.field("amount", pa.float64()),
    pa.field("interest_rate", pa.float64()),
    pa.field("is_small_value", pa.bool_()),
    pa.field("last_updated", pa.timestamp('s'))
])


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(httpx.HTTPError)
)
async def fetch_fx_swaps_data(client, endpoint):
    """Fetch FX Swaps data from NY Fed API"""
    url = f"{BASE_URL}/{endpoint}"
    response = await client.get(url, timeout=60)
    response.raise_for_status()
    return response.json()

def parse_date(date_str):
    """Parse date string to date object"""
    if not date_str:
        return None
    return datetime.strptime(date_str, "%Y-%m-%d").date()

def parse_timestamp(timestamp_str):
    """Parse timestamp string to datetime object"""
    if not timestamp_str:
        return None
    try:
        return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        # Try without seconds
        try:
            return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M")
        except ValueError:
            return None

def parse_bool(value):
    """Parse boolean value"""
    if value is None or value == "":
        return None
    return str(value).lower() in ['true', '1', 'yes']

async def fetch_historical_swaps(client, start_date, end_date):
    """Fetch historical FX swaps data"""
    records = []
    
    # Split into smaller chunks to avoid API limits (90 days seems safe)
    current_start = start_date
    while current_start <= end_date:
        # Calculate chunk end date (90 days or remaining period)
        chunk_end = min(
            current_start + timedelta(days=89),  # 90 days
            end_date
        )
        
        params = {
            "startDate": current_start.strftime("%Y-%m-%d"),
            "endDate": chunk_end.strftime("%Y-%m-%d")
        }
        
        print(f"  Fetching FX swaps for {params['startDate']} to {params['endDate']}")
        
        # Fetch swaps data
        data = await fetch_fx_swaps_data(client, f"fxs/all/search.json?startDate={params['startDate']}&endDate={params['endDate']}")
        
        if data and "fxSwaps" in data and "operations" in data["fxSwaps"]:
            for swap in data["fxSwaps"]["operations"]:
                record = {
                    "trade_date": parse_date(swap.get("tradeDate")),
                    "settlement_date": parse_date(swap.get("settlementDate")),
                    "maturity_date": parse_date(swap.get("maturityDate")),
                    "operation_type": swap.get("operationType"),
                    "counterparty": swap.get("counterparty"),
                    "currency": swap.get("currency"),
                    "term_days": int(swap.get("termInDays", 0)),
                    "amount": float(swap.get("amount", 0)),
                    "interest_rate": float(swap.get("interestRate", 0)),
                    "is_small_value": parse_bool(swap.get("isSmallValue")),
                    "last_updated": parse_timestamp(swap.get("lastUpdated"))
                }
                records.append(record)
        
        # Move to next chunk
        current_start = chunk_end + timedelta(days=1)
    
    return records

async def process_fx_swaps_async():
    """Process FX swaps data"""
    state = load_state("fx_swaps")
    last_date = state.get("last_date")
    
    # Determine date range to fetch
    if last_date:
        start_date = datetime.strptime(last_date, "%Y-%m-%d").date() + timedelta(days=1)
    else:
        # Start from 2020 for reasonable data volume
        start_date = datetime(2020, 1, 1).date()
    
    # End date is today
    end_date = datetime.now().date()
    
    if start_date > end_date:
        print("No new FX swaps data to fetch")
        return pa.Table.from_pylist([], schema=schema)
    
    print(f"Fetching FX swaps from {start_date} to {end_date}")
    
    async with httpx.AsyncClient() as client:
        records = await fetch_historical_swaps(client, start_date, end_date)
    
    if records:
        # Update state with the latest date
        max_date = max(r["trade_date"] for r in records if r["trade_date"])
        state["last_date"] = max_date.strftime("%Y-%m-%d")
        save_state("fx_swaps", state)
        
        print(f"Fetched {len(records)} FX swap records")
        return pa.Table.from_pylist(records, schema=schema)
    
    return pa.Table.from_pylist([], schema=schema)

def process_fx_swaps():
    """Synchronous wrapper for async processing"""
    return asyncio.run(process_fx_swaps_async())