
from datetime import datetime, timedelta
from subsets_utils import get, save_raw_json, load_state, save_state

BASE_URL = "https://markets.newyorkfed.org/api"


def fetch_rate_data(endpoint):
    """Fetch rate data from NY Fed API"""
    url = f"{BASE_URL}/{endpoint}"
    response = get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def run():
    """Fetch reference rates data and save raw JSON"""
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
        print("No new reference rates data to fetch")
        return

    print(f"Fetching reference rates from {start_date} to {end_date}")

    all_unsecured = []
    all_secured = []

    # Split into smaller chunks to avoid API limits (90 days seems safe)
    current_start = start_date
    while current_start <= end_date:
        chunk_end = min(current_start + timedelta(days=89), end_date)

        params = {
            "startDate": current_start.strftime("%Y-%m-%d"),
            "endDate": chunk_end.strftime("%Y-%m-%d")
        }

        print(f"  Fetching reference rates for {params['startDate']} to {params['endDate']}")

        # Fetch unsecured rates
        unsecured_data = fetch_rate_data(f"rates/all/search.json?startDate={params['startDate']}&endDate={params['endDate']}")
        if unsecured_data and "refRates" in unsecured_data:
            all_unsecured.extend(unsecured_data["refRates"])

        # Fetch secured rates
        secured_data = fetch_rate_data(f"rates/secured/all/search.json?startDate={params['startDate']}&endDate={params['endDate']}")
        if secured_data and "refRates" in secured_data:
            all_secured.extend(secured_data["refRates"])

        current_start = chunk_end + timedelta(days=1)

    # Save raw data
    raw_data = {
        "unsecured": all_unsecured,
        "secured": all_secured,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d")
    }
    save_raw_json(raw_data, "reference_rates")

    # Update state
    if all_unsecured or all_secured:
        save_state("reference_rates", {"last_date": end_date.strftime("%Y-%m-%d")})

    print(f"Fetched {len(all_unsecured)} unsecured + {len(all_secured)} secured rate records")
