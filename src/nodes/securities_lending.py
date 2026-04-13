"""NY Fed Securities Lending - ingest and transform.

Fetches and transforms securities lending operations.
Source: https://markets.newyorkfed.org/
"""

import asyncio
from datetime import datetime, timedelta
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import pyarrow as pa
from subsets_utils import save_raw_json, load_raw_json, load_state, save_state, merge, publish, validate

BASE_URL = "https://markets.newyorkfed.org/api"
DATASET_ID = "nyf_securities_lending"

METADATA = {
    "id": DATASET_ID,
    "title": "NY Fed Securities Lending Operations",
    "description": "Securities lending operations from the New York Fed's System Open Market Account (SOMA), providing individual security-level auction results.",
    "column_descriptions": {
        "operation_date": "Date the lending operation was conducted",
        "operation_id": "Unique identifier for the operation",
        "settlement_date": "Date the loan settles",
        "maturity_date": "Date the loan matures",
        "cusip": "CUSIP identifier of the lent security",
        "security_description": "Description of the lent security",
        "par_amount_submitted": "Par amount of bids submitted",
        "par_amount_accepted": "Par amount of bids accepted",
        "weighted_average_rate": "Weighted average lending fee rate",
        "soma_holdings": "SOMA holdings of the security",
        "theoretical_available": "Theoretical amount available to borrow",
        "actual_available": "Actual amount available to borrow",
        "outstanding_loans": "Amount of outstanding loans for the security",
        "release_time": "Time the operation was announced",
        "close_time": "Time the operation closed",
    },
}

SCHEMA = pa.schema([
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
    url = f"{BASE_URL}/{endpoint}"
    response = await client.get(url, timeout=60)
    response.raise_for_status()
    return response.json()


async def fetch_historical_operations_async(start_date, end_date):
    all_operations = []

    async with httpx.AsyncClient() as client:
        current_start = start_date
        while current_start <= end_date:
            chunk_end = min(current_start + timedelta(days=89), end_date)

            params = {
                "startDate": current_start.strftime("%Y-%m-%d"),
                "endDate": chunk_end.strftime("%Y-%m-%d")
            }

            print(f"    Fetching {params['startDate']} to {params['endDate']}")

            data = await fetch_seclending_data(
                client,
                f"seclending/all/results/details/search.json?startDate={params['startDate']}&endDate={params['endDate']}"
            )

            if data and "seclending" in data and "operations" in data["seclending"]:
                all_operations.extend(data["seclending"]["operations"])

            current_start = chunk_end + timedelta(days=1)

    return all_operations


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
            "cusip": "string",
        },
        "not_null": ["operation_date", "operation_id", "cusip"],
        "min_rows": 1,
    })
    print(f"  Validated {len(table):,} Securities Lending records")


def run():
    """Ingest and transform securities lending operations."""
    # Ingest
    print("Fetching NY Fed securities lending...")
    state = load_state("securities_lending")
    last_date = state.get("last_date")

    if last_date:
        start_date = datetime.strptime(last_date, "%Y-%m-%d").date() + timedelta(days=1)
    else:
        start_date = datetime(2020, 1, 1).date()

    end_date = datetime.now().date()

    if start_date > end_date:
        print("  No new securities lending data to fetch")
        return

    print(f"  Fetching from {start_date} to {end_date}")

    operations = asyncio.run(fetch_historical_operations_async(start_date, end_date))

    save_raw_json({
        "operations": operations,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d")
    }, "securities_lending")

    print(f"  Fetched {len(operations)} securities lending operations")

    # Transform
    print("Transforming securities lending...")
    records = []

    for operation in operations:
        if operation.get("auctionStatus") != "Results":
            continue

        operation_date = parse_date(operation.get("operationDate"))
        operation_id = operation.get("operationId")
        settlement_date = parse_date(operation.get("settlementDate"))
        maturity_date = parse_date(operation.get("maturityDate"))
        release_time = operation.get("releaseTime")
        close_time = operation.get("closeTime")

        details = operation.get("details", [])
        for detail in details:
            records.append({
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
            })

    if not records:
        print("  No Securities Lending records found")
        return

    print(f"  Transformed {len(records):,} records")
    table = pa.Table.from_pylist(records, schema=SCHEMA)

    test(table)
    merge(table, DATASET_ID, key=["operation_id", "cusip"])
    publish(DATASET_ID, METADATA)

    if operations:
        max_date = None
        for operation in operations:
            if operation.get("operationDate"):
                op_date = datetime.strptime(operation["operationDate"], "%Y-%m-%d").date()
                if max_date is None or op_date > max_date:
                    max_date = op_date
        if max_date:
            save_state("securities_lending", {"last_date": max_date.strftime("%Y-%m-%d")})


NODES = {
    run: [],
}

if __name__ == "__main__":
    run()
