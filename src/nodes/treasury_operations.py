"""NY Fed Treasury Operations - ingest and transform.

Fetches and transforms Treasury purchase and sale operations.
Source: https://markets.newyorkfed.org/
"""

import asyncio
from datetime import datetime, timedelta
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import pyarrow as pa
from subsets_utils import save_raw_json, load_raw_json, load_state, save_state, merge, publish, validate

BASE_URL = "https://markets.newyorkfed.org/api"
DATASET_ID = "nyf_treasury_operations"

METADATA = {
    "id": DATASET_ID,
    "title": "NY Fed Treasury Operations",
    "description": "Treasury securities purchase and sale operations conducted by the New York Fed, including outright purchases, sales, and TIPS/FRN operations.",
    "column_descriptions": {
        "operation_date": "Date the operation was conducted",
        "operation_id": "Unique identifier for the operation",
        "operation_type": "Type of operation (e.g., Outright TIPS Purchase, Outright FRN Sale)",
        "operation_direction": "Direction of the operation (P = purchase, S = sale)",
        "settlement_date": "Date the transaction settles",
        "cusip": "CUSIP identifier of the security",
        "security_description": "Description of the Treasury security",
        "maturity_date_start": "Start of the maturity range for the operation",
        "maturity_date_end": "End of the maturity range for the operation",
        "auction_method": "Auction method used (e.g., Multiple Price)",
        "par_amount_submitted": "Total par amount submitted across all securities",
        "par_amount_accepted": "Par amount accepted for this security",
        "weighted_avg_price": "Weighted average accepted price",
        "least_favorable_price": "Least favorable accepted price",
        "release_time": "Time the operation was announced",
        "close_time": "Time the operation closed",
    },
}

SCHEMA = pa.schema([
    pa.field("operation_date", pa.date32()),
    pa.field("operation_id", pa.string()),
    pa.field("operation_type", pa.string()),
    pa.field("operation_direction", pa.string()),
    pa.field("settlement_date", pa.date32()),
    pa.field("cusip", pa.string()),
    pa.field("security_description", pa.string()),
    pa.field("maturity_date_start", pa.date32()),
    pa.field("maturity_date_end", pa.date32()),
    pa.field("auction_method", pa.string()),
    pa.field("par_amount_submitted", pa.float64()),
    pa.field("par_amount_accepted", pa.float64()),
    pa.field("weighted_avg_price", pa.float64()),
    pa.field("least_favorable_price", pa.float64()),
    pa.field("release_time", pa.string()),
    pa.field("close_time", pa.string())
])


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(httpx.HTTPError)
)
async def fetch_treasury_data(client, endpoint):
    url = f"{BASE_URL}/{endpoint}"
    response = await client.get(url, timeout=60)
    response.raise_for_status()
    return response.json()


async def fetch_historical_operations_async(start_date, end_date):
    all_auctions = []

    async with httpx.AsyncClient() as client:
        current_start = start_date
        while current_start <= end_date:
            chunk_end = min(current_start + timedelta(days=89), end_date)

            params = {
                "startDate": current_start.strftime("%Y-%m-%d"),
                "endDate": chunk_end.strftime("%Y-%m-%d")
            }

            print(f"    Fetching {params['startDate']} to {params['endDate']}")

            data = await fetch_treasury_data(
                client,
                f"tsy/all/results/details/search.json?startDate={params['startDate']}&endDate={params['endDate']}"
            )

            if data and "treasury" in data and "auctions" in data["treasury"]:
                all_auctions.extend(data["treasury"]["auctions"])

            current_start = chunk_end + timedelta(days=1)

    return all_auctions


def parse_date(date_str):
    if not date_str:
        return None
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def parse_number(value):
    if value is None or value == "" or value == "NA":
        return None
    try:
        if isinstance(value, str):
            value = value.replace(",", "")
        return float(value)
    except (ValueError, TypeError):
        return None


def test(table: pa.Table) -> None:
    validate(table, {
        "columns": {
            "operation_date": "date",
            "operation_id": "string",
            "par_amount_accepted": "double",
        },
        "not_null": ["operation_date", "operation_id", "par_amount_accepted"],
        "min_rows": 1,
    })
    print(f"  Validated {len(table):,} Treasury operation records")


def run():
    """Ingest and transform Treasury operations."""
    # Ingest
    print("Fetching NY Fed Treasury operations...")
    state = load_state("treasury_operations")
    last_date = state.get("last_date")

    if last_date:
        start_date = datetime.strptime(last_date, "%Y-%m-%d").date() + timedelta(days=1)
    else:
        start_date = datetime(2020, 1, 1).date()

    end_date = datetime.now().date()

    if start_date > end_date:
        print("  No new Treasury operations data to fetch")
        return

    print(f"  Fetching from {start_date} to {end_date}")

    auctions = asyncio.run(fetch_historical_operations_async(start_date, end_date))

    save_raw_json({
        "auctions": auctions,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d")
    }, "treasury_operations")

    print(f"  Fetched {len(auctions)} Treasury auctions")

    # Transform
    print("Transforming Treasury operations...")
    records = []

    for auction in auctions:
        if auction.get("auctionStatus") != "Results":
            continue

        operation_date = parse_date(auction.get("operationDate"))
        details = auction.get("details", [])

        for detail in details:
            par_accepted = parse_number(detail.get("parAmountAccepted"))
            if par_accepted is None or par_accepted == 0:
                continue

            records.append({
                "operation_date": operation_date,
                "operation_id": auction.get("operationId"),
                "operation_type": auction.get("operationType"),
                "operation_direction": auction.get("operationDirection"),
                "settlement_date": parse_date(auction.get("settlementDate")),
                "cusip": detail.get("cusip"),
                "security_description": detail.get("securityDescription"),
                "maturity_date_start": parse_date(auction.get("maturityRangeStart")),
                "maturity_date_end": parse_date(auction.get("maturityRangeEnd")),
                "auction_method": auction.get("auctionMethod"),
                "par_amount_submitted": parse_number(auction.get("totalParAmtSubmitted")),
                "par_amount_accepted": par_accepted,
                "weighted_avg_price": parse_number(detail.get("weightedAvgAccptPrice")),
                "least_favorable_price": parse_number(detail.get("leastFavoriteAccptPrice")),
                "release_time": auction.get("releaseTime"),
                "close_time": auction.get("closeTime")
            })

    if not records:
        print("  No Treasury operation records found")
        return

    print(f"  Transformed {len(records):,} records")
    table = pa.Table.from_pylist(records, schema=SCHEMA)

    test(table)
    merge(table, DATASET_ID, key=["operation_id", "cusip"])
    publish(DATASET_ID, METADATA)

    if auctions:
        max_date = None
        for auction in auctions:
            if auction.get("operationDate"):
                op_date = datetime.strptime(auction["operationDate"], "%Y-%m-%d").date()
                if max_date is None or op_date > max_date:
                    max_date = op_date
        if max_date:
            save_state("treasury_operations", {"last_date": max_date.strftime("%Y-%m-%d")})


NODES = {
    run: [],
}

if __name__ == "__main__":
    run()
