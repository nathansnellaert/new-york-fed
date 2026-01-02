
import asyncio
from datetime import datetime, timedelta
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from subsets_utils import save_raw_json, load_state, save_state

BASE_URL = "https://markets.newyorkfed.org/api"


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


async def fetch_historical_swaps_async(start_date, end_date):
    """Fetch historical FX swaps data"""
    all_operations = []

    async with httpx.AsyncClient() as client:
        current_start = start_date
        while current_start <= end_date:
            chunk_end = min(current_start + timedelta(days=89), end_date)

            params = {
                "startDate": current_start.strftime("%Y-%m-%d"),
                "endDate": chunk_end.strftime("%Y-%m-%d")
            }

            print(f"  Fetching FX swaps for {params['startDate']} to {params['endDate']}")

            data = await fetch_fx_swaps_data(
                client,
                f"fxs/all/search.json?startDate={params['startDate']}&endDate={params['endDate']}"
            )

            if data and "fxSwaps" in data and "operations" in data["fxSwaps"]:
                all_operations.extend(data["fxSwaps"]["operations"])

            current_start = chunk_end + timedelta(days=1)

    return all_operations


def run():
    """Fetch FX swaps data and save raw JSON"""
    state = load_state("fx_swaps")
    last_date = state.get("last_date")

    # Determine date range to fetch
    if last_date:
        start_date = datetime.strptime(last_date, "%Y-%m-%d").date() + timedelta(days=1)
    else:
        start_date = datetime(2020, 1, 1).date()

    end_date = datetime.now().date()

    if start_date > end_date:
        print("No new FX swaps data to fetch")
        return

    print(f"Fetching FX swaps from {start_date} to {end_date}")

    operations = asyncio.run(fetch_historical_swaps_async(start_date, end_date))

    # Save raw data
    save_raw_json({
        "operations": operations,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d")
    }, "fx_swaps")

    # Update state
    if operations:
        max_date = None
        for op in operations:
            if op.get("tradeDate"):
                trade_date = datetime.strptime(op["tradeDate"], "%Y-%m-%d").date()
                if max_date is None or trade_date > max_date:
                    max_date = trade_date
        if max_date:
            save_state("fx_swaps", {"last_date": max_date.strftime("%Y-%m-%d")})

    print(f"Fetched {len(operations)} FX swap operations")
