"""NY Fed Repo Operations - ingest and transform.

Fetches and transforms repurchase agreement operations.
Source: https://markets.newyorkfed.org/
"""

import asyncio
from datetime import datetime, timedelta
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import pyarrow as pa
from subsets_utils import save_raw_json, load_raw_json, load_state, save_state, merge, publish, validate

BASE_URL = "https://markets.newyorkfed.org/api"
DATASET_ID = "nyf_repo_operations"

METADATA = {
    "id": DATASET_ID,
    "title": "NY Fed Repo Operations",
    "description": "Repurchase agreement (repo) and reverse repo operations conducted by the New York Fed.",
    "column_descriptions": {
        "operation_date": "Date the operation was conducted",
        "operation_id": "Unique identifier for the operation",
        "operation_type": "Type of operation (Repo or Reverse Repo)",
        "operation_method": "Auction method (e.g., Multiple Price, Fixed Rate, Full Allotment)",
        "settlement_date": "Date the transaction settles",
        "maturity_date": "Date the transaction matures",
        "term": "Term of the operation (Overnight or Term)",
        "term_calendar_days": "Number of calendar days in the term",
        "settlement_type": "Settlement timing (Same Day, 1 Day Forward)",
        "security_type": "Collateral type (Treasury, Agency, Mortgage-Backed)",
        "amount_submitted": "Amount submitted for this security type",
        "amount_accepted": "Amount accepted for this security type",
        "total_amount_submitted": "Total amount submitted across all security types",
        "total_amount_accepted": "Total amount accepted across all security types",
        "participating_counterparties": "Number of counterparties that submitted bids",
        "accepted_counterparties": "Number of counterparties with accepted bids",
        "offering_rate": "Rate offered by the Fed",
        "award_rate": "Rate at which awards were made",
        "weighted_average_rate": "Weighted average rate of accepted bids",
        "minimum_bid_rate": "Minimum bid rate",
        "maximum_bid_rate": "Maximum bid rate",
        "release_time": "Time the operation was announced",
        "close_time": "Time the operation closed",
    },
}

SCHEMA = pa.schema([
    pa.field("operation_date", pa.date32()),
    pa.field("operation_id", pa.string()),
    pa.field("operation_type", pa.string()),
    pa.field("operation_method", pa.string()),
    pa.field("settlement_date", pa.date32()),
    pa.field("maturity_date", pa.date32()),
    pa.field("term", pa.string()),
    pa.field("term_calendar_days", pa.int32()),
    pa.field("settlement_type", pa.string()),
    pa.field("security_type", pa.string()),
    pa.field("amount_submitted", pa.float64()),
    pa.field("amount_accepted", pa.float64()),
    pa.field("total_amount_submitted", pa.float64()),
    pa.field("total_amount_accepted", pa.float64()),
    pa.field("participating_counterparties", pa.int32()),
    pa.field("accepted_counterparties", pa.int32()),
    pa.field("offering_rate", pa.float64()),
    pa.field("award_rate", pa.float64()),
    pa.field("weighted_average_rate", pa.float64()),
    pa.field("minimum_bid_rate", pa.float64()),
    pa.field("maximum_bid_rate", pa.float64()),
    pa.field("release_time", pa.string()),
    pa.field("close_time", pa.string())
])


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(httpx.HTTPError)
)
async def fetch_repo_data(client, endpoint):
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

            data = await fetch_repo_data(
                client,
                f"rp/results/search.json?startDate={params['startDate']}&endDate={params['endDate']}"
            )

            if data and "repo" in data and "operations" in data["repo"]:
                all_operations.extend(data["repo"]["operations"])

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
        return float(value)
    except (ValueError, TypeError):
        return None


def parse_integer(value):
    if value is None or value == "":
        return None
    try:
        return int(value)
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
    print(f"  Validated {len(table):,} repo operation records")


def run():
    """Ingest and transform repo operations."""
    # Ingest
    print("Fetching NY Fed repo operations...")
    state = load_state("repo_operations")
    last_date = state.get("last_date")

    if last_date:
        start_date = datetime.strptime(last_date, "%Y-%m-%d").date() + timedelta(days=1)
    else:
        start_date = datetime(2020, 1, 1).date()

    end_date = datetime.now().date()

    if start_date > end_date:
        print("  No new repo operations data to fetch")
        return

    print(f"  Fetching from {start_date} to {end_date}")

    operations = asyncio.run(fetch_historical_operations_async(start_date, end_date))

    save_raw_json({
        "operations": operations,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d")
    }, "repo_operations")

    print(f"  Fetched {len(operations)} repo operations")

    # Transform
    print("Transforming repo operations...")
    records = []

    for operation in operations:
        if operation.get("auctionStatus") != "Results":
            continue

        operation_date = parse_date(operation.get("operationDate"))
        operation_id = operation.get("operationId")
        operation_type = operation.get("operationType")
        operation_method = operation.get("operationMethod")
        settlement_date = parse_date(operation.get("settlementDate"))
        maturity_date = parse_date(operation.get("maturityDate"))
        term = operation.get("term")
        term_calendar_days = parse_integer(operation.get("termCalenderDays"))
        settlement_type = operation.get("settlementType")
        total_amount_submitted = parse_number(operation.get("totalAmtSubmitted"))
        total_amount_accepted = parse_number(operation.get("totalAmtAccepted"))
        participating_cpty = parse_integer(operation.get("participatingCpty"))
        accepted_cpty = parse_integer(operation.get("acceptedCpty"))
        release_time = operation.get("releaseTime")
        close_time = operation.get("closeTime")

        details = operation.get("details", [])
        if details:
            for detail in details:
                records.append({
                    "operation_date": operation_date,
                    "operation_id": operation_id,
                    "operation_type": operation_type,
                    "operation_method": operation_method,
                    "settlement_date": settlement_date,
                    "maturity_date": maturity_date,
                    "term": term,
                    "term_calendar_days": term_calendar_days,
                    "settlement_type": settlement_type,
                    "security_type": detail.get("securityType"),
                    "amount_submitted": parse_number(detail.get("amtSubmitted")),
                    "amount_accepted": parse_number(detail.get("amtAccepted")),
                    "total_amount_submitted": total_amount_submitted,
                    "total_amount_accepted": total_amount_accepted,
                    "participating_counterparties": participating_cpty,
                    "accepted_counterparties": accepted_cpty,
                    "offering_rate": parse_number(detail.get("percentOfferingRate")),
                    "award_rate": parse_number(detail.get("percentAwardRate")),
                    "weighted_average_rate": parse_number(detail.get("percentWeightedAverageRate")),
                    "minimum_bid_rate": parse_number(detail.get("minimumBidRate")),
                    "maximum_bid_rate": parse_number(detail.get("maximumBidRate")),
                    "release_time": release_time,
                    "close_time": close_time
                })
        else:
            records.append({
                "operation_date": operation_date,
                "operation_id": operation_id,
                "operation_type": operation_type,
                "operation_method": operation_method,
                "settlement_date": settlement_date,
                "maturity_date": maturity_date,
                "term": term,
                "term_calendar_days": term_calendar_days,
                "settlement_type": settlement_type,
                "security_type": "Aggregate",
                "amount_submitted": total_amount_submitted,
                "amount_accepted": total_amount_accepted,
                "total_amount_submitted": total_amount_submitted,
                "total_amount_accepted": total_amount_accepted,
                "participating_counterparties": participating_cpty,
                "accepted_counterparties": accepted_cpty,
                "offering_rate": None,
                "award_rate": None,
                "weighted_average_rate": None,
                "minimum_bid_rate": None,
                "maximum_bid_rate": None,
                "release_time": release_time,
                "close_time": close_time
            })

    if not records:
        print("  No repo operation records found")
        return

    print(f"  Transformed {len(records):,} records")
    table = pa.Table.from_pylist(records, schema=SCHEMA)

    test(table)
    merge(table, DATASET_ID, key=["operation_id", "security_type"])
    publish(DATASET_ID, METADATA)

    if operations:
        max_date = None
        for operation in operations:
            if operation.get("operationDate"):
                op_date = datetime.strptime(operation["operationDate"], "%Y-%m-%d").date()
                if max_date is None or op_date > max_date:
                    max_date = op_date
        if max_date:
            save_state("repo_operations", {"last_date": max_date.strftime("%Y-%m-%d")})


NODES = {
    run: [],
}

if __name__ == "__main__":
    run()
