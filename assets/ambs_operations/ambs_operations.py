import asyncio
from datetime import datetime, timedelta
import httpx
import pyarrow as pa
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from utils.io import load_state, save_state

BASE_URL = "https://markets.newyorkfed.org/api"

schema = pa.schema([
    pa.field("operation_date", pa.date32()),
    pa.field("operation_id", pa.string()),
    pa.field("operation_type", pa.string()),
    pa.field("operation_direction", pa.string()),
    pa.field("settlement_date", pa.date32()),
    pa.field("security_description", pa.string()),
    pa.field("class_type", pa.string()),
    pa.field("method", pa.string()),
    pa.field("amount_submitted_par", pa.float64()),
    pa.field("amount_accepted_par", pa.float64()),
    pa.field("release_time", pa.string()),
    pa.field("close_time", pa.string()),
    pa.field("inclusion_flag", pa.string())
])


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(httpx.HTTPError)
)
async def fetch_ambs_data(client, endpoint):
    """Fetch AMBS data from NY Fed API"""
    url = f"{BASE_URL}/{endpoint}"
    response = await client.get(url, timeout=60)
    response.raise_for_status()
    return response.json()

def parse_date(date_str):
    """Parse date string to date object"""
    if not date_str:
        return None
    return datetime.strptime(date_str, "%Y-%m-%d").date()

def parse_number(value):
    """Parse numeric value, handling None, empty strings, and 'NA'"""
    if value is None or value == "" or value == "NA":
        return None
    try:
        # Remove commas if present
        if isinstance(value, str):
            value = value.replace(",", "")
        return float(value)
    except (ValueError, TypeError):
        return None

async def fetch_historical_operations(client, start_date, end_date):
    """Fetch historical AMBS operations data"""
    records = []
    
    # Split into smaller chunks to avoid API limits (90 days seems safe based on testing)
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
        
        print(f"  Fetching AMBS data for {params['startDate']} to {params['endDate']}")
        
        # Fetch operations data for this chunk
        data = await fetch_ambs_data(client, f"ambs/all/results/details/search.json?startDate={params['startDate']}&endDate={params['endDate']}")
        
        if data and "ambs" in data and "auctions" in data["ambs"]:
            for auction in data["ambs"]["auctions"]:
                # Skip if no results
                if auction.get("auctionStatus") != "Results":
                    continue
                    
                operation_date = parse_date(auction.get("operationDate"))
                
                # Process each detail within the auction
                details = auction.get("details", [])
                if details:
                    for detail in details:
                        record = {
                            "operation_date": operation_date,
                            "operation_id": auction.get("operationId"),
                            "operation_type": auction.get("operationType"),
                            "operation_direction": auction.get("operationDirection"),
                            "settlement_date": parse_date(auction.get("settlementDate")),
                            "security_description": detail.get("securityDescription"),
                            "class_type": auction.get("classType"),
                            "method": auction.get("method"),
                            "amount_submitted_par": parse_number(auction.get("totalAmtSubmittedPar")),
                            "amount_accepted_par": parse_number(detail.get("amtAcceptedPar")),
                            "release_time": auction.get("releaseTime"),
                            "close_time": auction.get("closeTime"),
                            "inclusion_flag": detail.get("inclusionExclusionFlag")
                        }
                        records.append(record)
                else:
                    # Create single record for auction without details
                    record = {
                        "operation_date": operation_date,
                        "operation_id": auction.get("operationId"),
                        "operation_type": auction.get("operationType"),
                        "operation_direction": auction.get("operationDirection"),
                        "settlement_date": parse_date(auction.get("settlementDate")),
                        "security_description": "Aggregate",
                        "class_type": auction.get("classType"),
                        "method": auction.get("method"),
                        "amount_submitted_par": parse_number(auction.get("totalAmtSubmittedPar")),
                        "amount_accepted_par": parse_number(auction.get("totalAmtAcceptedPar")),
                        "release_time": auction.get("releaseTime"),
                        "close_time": auction.get("closeTime"),
                        "inclusion_flag": None
                    }
                    records.append(record)
        
        # Move to next chunk
        current_start = chunk_end + timedelta(days=1)
    
    return records

async def process_ambs_operations_async():
    """Process AMBS operations data"""
    state = load_state("ambs_operations")
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
        print("No new AMBS operations data to fetch")
        return pa.Table.from_pylist([], schema=schema)
    
    print(f"Fetching AMBS operations from {start_date} to {end_date}")
    
    async with httpx.AsyncClient() as client:
        records = await fetch_historical_operations(client, start_date, end_date)
    
    if records:
        # Update state with the latest date
        max_date = max(r["operation_date"] for r in records if r["operation_date"])
        state["last_date"] = max_date.strftime("%Y-%m-%d")
        save_state("ambs_operations", state)
        
        print(f"Fetched {len(records)} AMBS operation records")
        return pa.Table.from_pylist(records, schema=schema)
    
    return pa.Table.from_pylist([], schema=schema)

def process_ambs_operations():
    """Synchronous wrapper for async processing"""
    return asyncio.run(process_ambs_operations_async())