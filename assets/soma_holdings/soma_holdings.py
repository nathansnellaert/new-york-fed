import asyncio
from datetime import datetime
import httpx
import pyarrow as pa
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from utils.io import load_state, save_state

BASE_URL = "https://markets.newyorkfed.org/api"

schema = pa.schema([
    pa.field("as_of_date", pa.date32()),
    pa.field("security_type", pa.string()),
    pa.field("cusip", pa.string()),
    pa.field("security_description", pa.string()),
    pa.field("maturity_date", pa.date32()),
    pa.field("issuer", pa.string()),
    pa.field("coupon_rate", pa.float64()),
    pa.field("par_value", pa.float64()),
    pa.field("percent_outstanding", pa.float64()),
    pa.field("change_from_prior_week", pa.float64()),
    pa.field("change_from_prior_year", pa.float64())
])


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(httpx.HTTPError)
)
async def fetch_soma_data(client, endpoint):
    """Fetch SOMA data from NY Fed API"""
    url = f"{BASE_URL}/{endpoint}"
    response = await client.get(url, timeout=60)
    response.raise_for_status()
    return response.json()

def parse_date(date_str):
    """Parse date string to date object"""
    if not date_str:
        return None
    # Handle both YYYY-MM-DD and MM/DD/YYYY formats
    for fmt in ["%Y-%m-%d", "%m/%d/%Y"]:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    return None

def parse_number(value):
    """Parse numeric value, handling None, empty strings, and 'NA'"""
    if value is None or value == "" or value == "NA":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def determine_security_type(security_desc):
    """Determine security type from description"""
    desc_upper = security_desc.upper()
    if "BILL" in desc_upper or "T-BILL" in desc_upper:
        return "Treasury Bill"
    elif "NOTE" in desc_upper or "T-NOTE" in desc_upper:
        return "Treasury Note"
    elif "BOND" in desc_upper or "T-BOND" in desc_upper:
        return "Treasury Bond"
    elif "TIPS" in desc_upper:
        return "Treasury Inflation-Protected"
    elif "FRN" in desc_upper:
        return "Floating Rate Note"
    else:
        return "Treasury Security"

async def fetch_latest_holdings(client):
    """Fetch the latest SOMA holdings data"""
    records = []
    
    # Get latest available date
    latest_data = await fetch_soma_data(client, "soma/summary.json")
    if not latest_data or "soma" not in latest_data or "summary" not in latest_data["soma"]:
        return records, None
    
    # Get the most recent date (last entry in the summary list)
    summary_list = latest_data["soma"]["summary"]
    if not summary_list:
        return records, None
        
    as_of_date = parse_date(summary_list[-1]["asOfDate"])
    
    # Fetch Treasury holdings - use CSV endpoint as JSON seems problematic
    treasury_data = await fetch_soma_data(client, f"soma/tsy/get/all/asof/{as_of_date.strftime('%Y-%m-%d')}.json")
    
    if treasury_data and "soma" in treasury_data and "holdings" in treasury_data["soma"]:
        for holding in treasury_data["soma"]["holdings"]:
            record = {
                "as_of_date": as_of_date,
                "security_type": determine_security_type(holding.get("securityDescription", "")),
                "cusip": holding.get("cusip", ""),
                "security_description": holding.get("securityDescription", ""),
                "maturity_date": parse_date(holding.get("maturityDate")),
                "issuer": "U.S. Treasury",
                "coupon_rate": parse_number(holding.get("couponPercent")),
                "par_value": parse_number(holding.get("parValue")),
                "percent_outstanding": parse_number(holding.get("percentOutstanding")),
                "change_from_prior_week": parse_number(holding.get("changeFromPriorWeek")),
                "change_from_prior_year": parse_number(holding.get("changeFromPriorYear"))
            }
            records.append(record)
    
    # Fetch Agency holdings
    agency_data = await fetch_soma_data(client, f"soma/agency/get/asof/{as_of_date.strftime('%Y-%m-%d')}.json")
    
    if agency_data and "soma" in agency_data and "holdings" in agency_data["soma"]:
        for holding in agency_data["soma"]["holdings"]:
            record = {
                "as_of_date": as_of_date,
                "security_type": "Agency Debt",
                "cusip": holding.get("cusip", ""),
                "security_description": holding.get("securityDescription", ""),
                "maturity_date": parse_date(holding.get("maturityDate")),
                "issuer": holding.get("issuer", ""),
                "coupon_rate": parse_number(holding.get("couponPercent")),
                "par_value": parse_number(holding.get("parValue")),
                "percent_outstanding": parse_number(holding.get("percentOutstanding")),
                "change_from_prior_week": parse_number(holding.get("changeFromPriorWeek")),
                "change_from_prior_year": parse_number(holding.get("changeFromPriorYear"))
            }
            records.append(record)
    
    return records, as_of_date

async def process_soma_holdings_async():
    """Process SOMA holdings data"""
    state = load_state("soma_holdings")
    last_date = state.get("last_date")
    
    async with httpx.AsyncClient() as client:
        records, current_date = await fetch_latest_holdings(client)
    
    if not records or current_date is None:
        print("No SOMA holdings data available")
        return pa.Table.from_pylist([], schema=schema)
    
    # Check if we have new data
    if last_date and last_date == current_date.strftime("%Y-%m-%d"):
        print("No new SOMA holdings data since last run")
        return pa.Table.from_pylist([], schema=schema)
    
    # Update state
    state["last_date"] = current_date.strftime("%Y-%m-%d")
    save_state("soma_holdings", state)
    
    print(f"Fetched {len(records)} SOMA holdings records for {current_date}")
    return pa.Table.from_pylist(records, schema=schema)

def process_soma_holdings():
    """Synchronous wrapper for async processing"""
    return asyncio.run(process_soma_holdings_async())