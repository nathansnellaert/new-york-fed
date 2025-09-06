from datetime import datetime, timedelta
import pyarrow as pa
from utils.http_client import get
from utils.io import load_state, save_state

BASE_URL = "https://markets.newyorkfed.org/api"

# Define schema
schema = pa.schema([
    pa.field("date", pa.date32(), nullable=False),
    pa.field("rate_type", pa.string(), nullable=False),
    pa.field("percentile_1", pa.float64(), nullable=True),
    pa.field("percentile_25", pa.float64(), nullable=True),
    pa.field("percentile_75", pa.float64(), nullable=True),
    pa.field("percentile_99", pa.float64(), nullable=True),
    pa.field("rate", pa.float64(), nullable=False),
    pa.field("volume_billions", pa.float64(), nullable=True),
    pa.field("target_rate_from", pa.float64(), nullable=True),
    pa.field("target_rate_to", pa.float64(), nullable=True)
])


def fetch_rate_data(endpoint):
    """Fetch rate data from NY Fed API"""
    url = f"{BASE_URL}/{endpoint}"
    response = get(url, timeout=30)
    response.raise_for_status()
    return response.json()

def parse_date(date_str):
    """Parse date string to date object"""
    return datetime.strptime(date_str, "%Y-%m-%d").date()

def parse_number(value):
    """Parse numeric value, handling None and 'NA'"""
    if value is None or value == "NA":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def process_rate_record(rate_data):
    """Process a single rate record from the API"""
    date = parse_date(rate_data["effectiveDate"])
    rate_type = rate_data["type"]
    
    # Map API rate types to our schema
    type_mapping = {
        "EFFR": "EFFR",
        "OBFR": "OBFR", 
        "SOFR": "SOFR",
        "BGCR": "BGCR",
        "TGCR": "TGCR"
    }
    
    if rate_type not in type_mapping:
        return None
    
    return {
        "date": date,
        "rate_type": type_mapping[rate_type],
        "percentile_1": parse_number(rate_data.get("percentPercentile1")),
        "percentile_25": parse_number(rate_data.get("percentPercentile25")),
        "percentile_75": parse_number(rate_data.get("percentPercentile75")),
        "percentile_99": parse_number(rate_data.get("percentPercentile99")),
        "rate": parse_number(rate_data.get("percentRate")),
        "volume_billions": parse_number(rate_data.get("volumeInBillions")),
        "target_rate_from": parse_number(rate_data.get("targetRateFrom")),
        "target_rate_to": parse_number(rate_data.get("targetRateTo"))
    }

def fetch_historical_data(start_date, end_date):
    """Fetch historical data for a date range"""
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
        
        print(f"  Fetching reference rates for {params['startDate']} to {params['endDate']}")
        
        # Fetch unsecured rates
        unsecured_data = fetch_rate_data(f"rates/all/search.json?startDate={params['startDate']}&endDate={params['endDate']}")
        
        # Fetch secured rates
        secured_data = fetch_rate_data(f"rates/secured/all/search.json?startDate={params['startDate']}&endDate={params['endDate']}")
        
        # Process unsecured rates
        if unsecured_data and "refRates" in unsecured_data:
            for rate_data in unsecured_data["refRates"]:
                record = process_rate_record(rate_data)
                if record:
                    records.append(record)
        
        # Process secured rates
        if secured_data and "refRates" in secured_data:
            for rate_data in secured_data["refRates"]:
                record = process_rate_record(rate_data)
                if record:
                    records.append(record)
        
        # Move to next chunk
        current_start = chunk_end + timedelta(days=1)
    
    return records

def process_reference_rates():
    """Process reference rates data"""
    state = load_state("reference_rates")
    last_date = state.get("last_date")
    
    # Determine date range to fetch
    if last_date:
        start_date = datetime.strptime(last_date, "%Y-%m-%d").date() + timedelta(days=1)
    else:
        # Start from 2018 when SOFR was introduced
        start_date = datetime(2018, 4, 3).date()
    
    # End date is yesterday (data is published with 1-day lag)
    end_date = datetime.now().date() - timedelta(days=1)
    
    if start_date > end_date:
        print("No new data to fetch")
        return pa.Table.from_pylist([], schema=schema)
    
    print(f"Fetching reference rates from {start_date} to {end_date}")
    
    records = fetch_historical_data(start_date, end_date)
    
    if records:
        # Update state with the latest date
        max_date = max(r["date"] for r in records)
        save_state("reference_rates", {"last_date": max_date.strftime("%Y-%m-%d")})
        
        print(f"Fetched {len(records)} reference rate records")
        return pa.Table.from_pylist(records, schema=schema)
    
    return pa.Table.from_pylist([], schema=schema)