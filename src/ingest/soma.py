
import asyncio
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from subsets_utils import save_raw_json, load_state, save_state

BASE_URL = "https://markets.newyorkfed.org/api"


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


async def fetch_holdings_async():
    """Fetch the latest SOMA holdings data"""
    async with httpx.AsyncClient() as client:
        # Get latest available date
        latest_data = await fetch_soma_data(client, "soma/summary.json")
        if not latest_data or "soma" not in latest_data or "summary" not in latest_data["soma"]:
            return None, None

        summary_list = latest_data["soma"]["summary"]
        if not summary_list:
            return None, None

        as_of_date = summary_list[-1]["asOfDate"]

        # Fetch Treasury holdings
        treasury_data = await fetch_soma_data(client, f"soma/tsy/get/all/asof/{as_of_date}.json")

        # Fetch Agency holdings
        agency_data = await fetch_soma_data(client, f"soma/agency/get/asof/{as_of_date}.json")

        return {
            "as_of_date": as_of_date,
            "summary": latest_data,
            "treasury": treasury_data,
            "agency": agency_data
        }, as_of_date


def run():
    """Fetch SOMA holdings data and save raw JSON"""
    state = load_state("soma_holdings")
    last_date = state.get("last_date")

    raw_data, current_date = asyncio.run(fetch_holdings_async())

    if not raw_data or current_date is None:
        print("No SOMA holdings data available")
        return

    # Check if we have new data
    if last_date and last_date == current_date:
        print("No new SOMA holdings data since last run")
        return

    # Save raw data
    save_raw_json(raw_data, "soma_holdings")

    # Update state
    save_state("soma_holdings", {"last_date": current_date})

    treasury_count = len(raw_data.get("treasury", {}).get("soma", {}).get("holdings", []))
    agency_count = len(raw_data.get("agency", {}).get("soma", {}).get("holdings", []))
    print(f"Fetched SOMA holdings for {current_date}: {treasury_count} treasury + {agency_count} agency")
