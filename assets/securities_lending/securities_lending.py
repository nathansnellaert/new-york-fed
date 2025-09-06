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
    pa.field("settlement_date", pa.date32()),
    pa.field("maturity_date", pa.date32()),
    pa.field("cusip", pa.string()),
    pa.field("security_description", pa.string()),
    pa.field("par_amount_submitted", pa.float64()),
    pa.field("par_amount_accepted", pa.float64()),
    pa.field("weighted_average_rate", pa.float64()),
    pa.field("soma_holdings", pa.float64()),
    pa.field("theoretical_available", pa.float64()),
    pa.field("actual_available", pa.float64()),
    pa.field("outstanding_loans", pa.float64()),
    pa.field("release_time", pa.string()),
    pa.field("close_time", pa.string())
])


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(httpx.HTTPError)
)
async def fetch_seclending_data(client, endpoint):
    """Fetch Securities Lending data from NY Fed API"""
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
    """Fetch historical Securities Lending operations data"""
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
        
        print(f"  Fetching Securities Lending data for {params['startDate']} to {params['endDate']}")
        
        # Fetch operations data
        data = await fetch_seclending_data(client, f"seclending/all/results/details/search.json?startDate={params['startDate']}&endDate={params['endDate']}")
        
        if data and "seclending" in data and "operations" in data["seclending"]:
            for operation in data["seclending"]["operations"]:
                # Skip if no results
                if operation.get("auctionStatus") != "Results":
                    continue
                    
                operation_date = parse_date(operation.get("operationDate"))
                operation_id = operation.get("operationId")
                settlement_date = parse_date(operation.get("settlementDate"))
                maturity_date = parse_date(operation.get("maturityDate"))
                release_time = operation.get("releaseTime")
                close_time = operation.get("closeTime")
                
                # Process each detail within the operation
                details = operation.get("details", [])
                if details:
                    for detail in details:
                        record = {
                            "operation_date": operation_date,
                            "operation_id": operation_id,
                            "settlement_date": settlement_date,
                            "maturity_date": maturity_date,
                            "cusip": detail.get("cusip"),
                            "security_description": detail.get("securityDescription"),
                            "par_amount_submitted": parse_number(detail.get("parAmtSubmitted")),
                            "par_amount_accepted": parse_number(detail.get("parAmtAccepted")),
                            "weighted_average_rate": parse_number(detail.get("weightedAverageRate")),
                            "soma_holdings": parse_number(detail.get("somaHoldings")),
                            "theoretical_available": parse_number(detail.get("theoAvailToBorrow")),
                            "actual_available": parse_number(detail.get("actualAvailToBorrow")),
                            "outstanding_loans": parse_number(detail.get("outstandingLoans")),
                            "release_time": release_time,
                            "close_time": close_time
                        }
                        records.append(record)
        
        # Move to next chunk
        current_start = chunk_end + timedelta(days=1)
    
    return records

async def process_securities_lending_async():
    """Process Securities Lending operations data"""
    state = load_state("securities_lending")
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
        print("No new Securities Lending operations data to fetch")
        return pa.Table.from_pylist([], schema=schema)
    
    print(f"Fetching Securities Lending operations from {start_date} to {end_date}")
    
    async with httpx.AsyncClient() as client:
        records = await fetch_historical_operations(client, start_date, end_date)
    
    if records:
        # Update state with the latest date
        max_date = max(r["operation_date"] for r in records if r["operation_date"])
        state["last_date"] = max_date.strftime("%Y-%m-%d")
        save_state("securities_lending", state)
        
        print(f"Fetched {len(records)} Securities Lending records")
        return pa.Table.from_pylist(records, schema=schema)
    
    return pa.Table.from_pylist([], schema=schema)

def process_securities_lending():
    """Synchronous wrapper for async processing"""
    return asyncio.run(process_securities_lending_async())