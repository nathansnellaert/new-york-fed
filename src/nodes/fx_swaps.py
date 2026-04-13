"""NY Fed FX Swaps - ingest and transform.

Fetches and transforms foreign exchange swap operations.
Source: https://markets.newyorkfed.org/
"""

import asyncio
from datetime import datetime, timedelta
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import pyarrow as pa
from subsets_utils import save_raw_json, load_raw_json, load_state, save_state, merge, publish, validate

BASE_URL = "https://markets.newyorkfed.org/api"
DATASET_ID = "nyf_fx_swaps"

METADATA = {
    "id": DATASET_ID,
    "title": "NY Fed FX Swap Operations",
    "description": "Foreign exchange swap operations conducted by the New York Fed with central bank counterparties.",
    "column_descriptions": {
        "trade_date": "Date the swap was traded",
        "settlement_date": "Date the swap settles",
        "maturity_date": "Date the swap matures",
        "operation_type": "Type of liquidity operation (e.g., U.S. Dollar Liquidity, Non-U.S. Dollar Liquidity)",
        "counterparty": "Central bank counterparty (e.g., European Central Bank, Bank of Canada)",
        "currency": "Currency of the swap (e.g., EUR, CAD, JPY, USD)",
        "term_days": "Term of the swap in calendar days",
        "amount": "Notional amount of the swap",
        "interest_rate": "Interest rate on the swap",
        "is_small_value": "Whether the operation is a small-value exercise",
        "last_updated": "Timestamp when the record was last updated",
    },
}

SCHEMA = pa.schema([
    pa.field("trade_date", pa.date32()),
    pa.field("settlement_date", pa.date32()),
    pa.field("maturity_date", pa.date32()),
    pa.field("operation_type", pa.string()),
    pa.field("counterparty", pa.string()),
    pa.field("currency", pa.string()),
    pa.field("term_days", pa.int32()),
    pa.field("amount", pa.float64()),
    pa.field("interest_rate", pa.float64()),
    pa.field("is_small_value", pa.bool_()),
    pa.field("last_updated", pa.timestamp('s'))
])


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(httpx.HTTPError)
)
async def fetch_fx_swaps_data(client, endpoint):
    url = f"{BASE_URL}/{endpoint}"
    response = await client.get(url, timeout=60)
    response.raise_for_status()
    return response.json()


async def fetch_historical_swaps_async(start_date, end_date):
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

            data = await fetch_fx_swaps_data(
                client,
                f"fxs/all/search.json?startDate={params['startDate']}&endDate={params['endDate']}"
            )

            if data and "fxSwaps" in data and "operations" in data["fxSwaps"]:
                all_operations.extend(data["fxSwaps"]["operations"])

            current_start = chunk_end + timedelta(days=1)

    return all_operations


def parse_date(date_str):
    if not date_str:
        return None
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def parse_timestamp(timestamp_str):
    if not timestamp_str:
        return None
    try:
        return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M")
        except ValueError:
            return None


def parse_bool(value):
    if value is None or value == "":
        return None
    return str(value).lower() in ['true', '1', 'yes']


def test(table: pa.Table) -> None:
    validate(table, {
        "columns": {
            "trade_date": "date",
            "currency": "string",
        },
        "not_null": ["trade_date", "currency"],
        "min_rows": 1,
    })
    print(f"  Validated {len(table):,} FX swap records")


def run():
    """Ingest and transform FX swaps."""
    # Ingest
    print("Fetching NY Fed FX swaps...")
    state = load_state("fx_swaps")
    last_date = state.get("last_date")

    if last_date:
        start_date = datetime.strptime(last_date, "%Y-%m-%d").date() + timedelta(days=1)
    else:
        start_date = datetime(2020, 1, 1).date()

    end_date = datetime.now().date()

    if start_date > end_date:
        print("  No new FX swaps data to fetch")
        return

    print(f"  Fetching from {start_date} to {end_date}")

    operations = asyncio.run(fetch_historical_swaps_async(start_date, end_date))

    save_raw_json({
        "operations": operations,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d")
    }, "fx_swaps")

    print(f"  Fetched {len(operations)} FX swap operations")

    # Transform
    print("Transforming FX swaps...")
    records = []

    for swap in operations:
        records.append({
            "trade_date": parse_date(swap.get("tradeDate")),
            "settlement_date": parse_date(swap.get("settlementDate")),
            "maturity_date": parse_date(swap.get("maturityDate")),
            "operation_type": swap.get("operationType"),
            "counterparty": swap.get("counterparty"),
            "currency": swap.get("currency"),
            "term_days": int(swap.get("termInDays", 0)) if swap.get("termInDays") else None,
            "amount": float(swap.get("amount", 0)) if swap.get("amount") else None,
            "interest_rate": float(swap.get("interestRate", 0)) if swap.get("interestRate") else None,
            "is_small_value": parse_bool(swap.get("isSmallValue")),
            "last_updated": parse_timestamp(swap.get("lastUpdated"))
        })

    if not records:
        print("  No FX swap records found")
        return

    print(f"  Transformed {len(records):,} records")
    table = pa.Table.from_pylist(records, schema=SCHEMA)

    test(table)
    merge(table, DATASET_ID, key=["trade_date", "settlement_date", "maturity_date", "currency"])
    publish(DATASET_ID, METADATA)

    if operations:
        max_date = None
        for op in operations:
            if op.get("tradeDate"):
                trade_date = datetime.strptime(op["tradeDate"], "%Y-%m-%d").date()
                if max_date is None or trade_date > max_date:
                    max_date = trade_date
        if max_date:
            save_state("fx_swaps", {"last_date": max_date.strftime("%Y-%m-%d")})


NODES = {
    run: [],
}

if __name__ == "__main__":
    run()
