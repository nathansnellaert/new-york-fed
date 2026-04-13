"""NY Fed AMBS Operations - ingest and transform.

Fetches and transforms Agency MBS operations data.
Source: https://markets.newyorkfed.org/
"""

import asyncio
from datetime import datetime, timedelta
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import pyarrow as pa
from subsets_utils import save_raw_json, load_raw_json, load_state, save_state, overwrite, publish, validate

BASE_URL = "https://markets.newyorkfed.org/api"
DATASET_ID = "nyf_ambs_operations"

METADATA = {
    "id": DATASET_ID,
    "title": "NY Fed AMBS Operations",
    "description": "Agency mortgage-backed securities (AMBS) purchase and sale operations conducted by the New York Fed.",
    "column_descriptions": {
        "operation_date": "Date the operation was conducted",
        "operation_id": "Unique identifier for the operation",
        "operation_type": "Type of operation (e.g., Coupon Swap, Dollar Roll, Outright)",
        "operation_direction": "Direction of the operation (P = purchase, S = sale)",
        "settlement_date": "Date the transaction settles",
        "security_description": "Description of the MBS security traded",
        "class_type": "MBS class type (A, B, or C)",
        "method": "Auction method used (e.g., Multiple Price)",
        "amount_submitted_par": "Total par amount submitted in the operation",
        "amount_accepted_par": "Par amount accepted in the operation",
        "release_time": "Time the operation was announced",
        "close_time": "Time the operation closed",
        "inclusion_flag": "Flag indicating inclusion or exclusion status",
    },
}

SCHEMA = pa.schema([
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

            data = await fetch_ambs_data(
                client,
                f"ambs/all/results/details/search.json?startDate={params['startDate']}&endDate={params['endDate']}"
            )

            if data and "ambs" in data and "auctions" in data["ambs"]:
                all_auctions.extend(data["ambs"]["auctions"])

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
        },
        "not_null": ["operation_date", "operation_id"],
        "min_rows": 1,
    })
    print(f"  Validated {len(table):,} AMBS operation records")


def run():
    """Ingest and transform AMBS operations."""
    # Ingest
    print("Fetching NY Fed AMBS operations...")
    state = load_state("ambs_operations")
    last_date = state.get("last_date")

    if last_date:
        start_date = datetime.strptime(last_date, "%Y-%m-%d").date() + timedelta(days=1)
    else:
        start_date = datetime(2020, 1, 1).date()

    end_date = datetime.now().date()

    if start_date > end_date:
        print("  No new AMBS operations data to fetch")
        return

    print(f"  Fetching from {start_date} to {end_date}")

    auctions = asyncio.run(fetch_historical_operations_async(start_date, end_date))

    save_raw_json({
        "auctions": auctions,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d")
    }, "ambs_operations")

    print(f"  Fetched {len(auctions)} AMBS auctions")

    # Transform
    print("Transforming AMBS operations...")
    records = []

    for auction in auctions:
        if auction.get("auctionStatus") != "Results":
            continue

        operation_date = parse_date(auction.get("operationDate"))
        details = auction.get("details", [])

        if details:
            for i, detail in enumerate(details):
                records.append({
                    "operation_date": operation_date,
                    "operation_id": auction.get("operationId"),
                    "operation_type": auction.get("operationType"),
                    "operation_direction": detail.get("operationDirection") or auction.get("operationDirection"),
                    "settlement_date": parse_date(auction.get("settlementDate")),
                    "security_description": detail.get("securityDescription"),
                    "class_type": auction.get("classType"),
                    "method": auction.get("method"),
                    "amount_submitted_par": parse_number(auction.get("totalAmtSubmittedPar")),
                    "amount_accepted_par": parse_number(detail.get("amtAcceptedPar")),
                    "release_time": auction.get("releaseTime"),
                    "close_time": auction.get("closeTime"),
                    "inclusion_flag": detail.get("inclusionExclusionFlag")
                })
        else:
            records.append({
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
            })

    if not records:
        print("  No AMBS operation records found")
        return

    print(f"  Transformed {len(records):,} records")
    table = pa.Table.from_pylist(records, schema=SCHEMA)

    test(table)
    overwrite(table, DATASET_ID)
    publish(DATASET_ID, METADATA)

    if auctions:
        max_date = None
        for auction in auctions:
            if auction.get("operationDate"):
                op_date = datetime.strptime(auction["operationDate"], "%Y-%m-%d").date()
                if max_date is None or op_date > max_date:
                    max_date = op_date
        if max_date:
            save_state("ambs_operations", {"last_date": max_date.strftime("%Y-%m-%d")})


NODES = {
    run: [],
}

if __name__ == "__main__":
    run()
