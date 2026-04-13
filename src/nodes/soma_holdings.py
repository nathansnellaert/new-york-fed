"""NY Fed SOMA Holdings - ingest and transform.

Fetches and transforms System Open Market Account holdings.
Source: https://markets.newyorkfed.org/
"""

import asyncio
from datetime import datetime
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import pyarrow as pa
from subsets_utils import save_raw_json, load_raw_json, load_state, save_state, merge, publish, validate

BASE_URL = "https://markets.newyorkfed.org/api"
DATASET_ID = "nyf_soma_holdings"

METADATA = {
    "id": DATASET_ID,
    "title": "NY Fed SOMA Holdings",
    "description": "System Open Market Account (SOMA) holdings of Treasury and agency securities held by the New York Fed.",
    "column_descriptions": {
        "as_of_date": "Date the holdings snapshot was taken",
        "security_type": "Type of security (e.g., Treasury Bill, Treasury Note, Treasury Bond, Agency Debt)",
        "cusip": "CUSIP identifier of the security",
        "security_description": "Description of the security",
        "maturity_date": "Maturity date of the security",
        "issuer": "Issuer of the security (e.g., U.S. Treasury)",
        "coupon_rate": "Coupon rate of the security (percent)",
        "par_value": "Par value of SOMA holdings",
        "percent_outstanding": "SOMA holdings as a percentage of total outstanding",
        "change_from_prior_week": "Change in par value from prior week",
        "change_from_prior_year": "Change in par value from prior year",
    },
}

SCHEMA = pa.schema([
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
    url = f"{BASE_URL}/{endpoint}"
    response = await client.get(url, timeout=60)
    response.raise_for_status()
    return response.json()


async def fetch_holdings_async():
    async with httpx.AsyncClient() as client:
        latest_data = await fetch_soma_data(client, "soma/summary.json")
        if not latest_data or "soma" not in latest_data or "summary" not in latest_data["soma"]:
            return None, None

        summary_list = latest_data["soma"]["summary"]
        if not summary_list:
            return None, None

        as_of_date = summary_list[-1]["asOfDate"]
        treasury_data = await fetch_soma_data(client, f"soma/tsy/get/all/asof/{as_of_date}.json")
        agency_data = await fetch_soma_data(client, f"soma/agency/get/asof/{as_of_date}.json")

        return {
            "as_of_date": as_of_date,
            "summary": latest_data,
            "treasury": treasury_data,
            "agency": agency_data
        }, as_of_date


def parse_date(date_str):
    if not date_str:
        return None
    for fmt in ["%Y-%m-%d", "%m/%d/%Y"]:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    return None


def parse_number(value):
    if value is None or value == "" or value == "NA":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def determine_security_type(security_desc):
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
    return "Treasury Security"


def test(table: pa.Table) -> None:
    validate(table, {
        "columns": {
            "as_of_date": "date",
            "security_type": "string",
            "cusip": "string",
        },
        "not_null": ["as_of_date", "security_type", "cusip"],
        "min_rows": 1,
    })

    security_types = set(table.column("security_type").to_pylist())
    assert len(security_types) >= 1, "Should have at least one security type"

    print(f"  Validated {len(table):,} SOMA holdings records")


def run():
    """Ingest and transform SOMA holdings."""
    # Ingest
    print("Fetching NY Fed SOMA holdings...")
    state = load_state("soma_holdings")
    last_date = state.get("last_date")

    raw_data, current_date = asyncio.run(fetch_holdings_async())

    if not raw_data or current_date is None:
        print("  No SOMA holdings data available")
        return

    if last_date and last_date == current_date:
        print("  No new SOMA holdings data since last run")
        return

    save_raw_json(raw_data, "soma_holdings")

    treasury_count = len(raw_data.get("treasury", {}).get("soma", {}).get("holdings", []))
    agency_count = len(raw_data.get("agency", {}).get("soma", {}).get("holdings", []))
    print(f"  Fetched SOMA holdings for {current_date}: {treasury_count} treasury + {agency_count} agency")

    # Transform
    print("Transforming SOMA holdings...")
    if not raw_data.get("treasury") and not raw_data.get("agency"):
        print("  No SOMA holdings data found")
        return

    as_of_date = parse_date(raw_data.get("as_of_date"))
    records = []

    treasury_data = raw_data.get("treasury", {})
    if treasury_data and "soma" in treasury_data and "holdings" in treasury_data["soma"]:
        for holding in treasury_data["soma"]["holdings"]:
            records.append({
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
            })

    agency_data = raw_data.get("agency", {})
    if agency_data and "soma" in agency_data and "holdings" in agency_data["soma"]:
        for holding in agency_data["soma"]["holdings"]:
            records.append({
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
            })

    if not records:
        print("  No SOMA holdings records found")
        return

    print(f"  Transformed {len(records):,} records")
    table = pa.Table.from_pylist(records, schema=SCHEMA)

    test(table)
    merge(table, DATASET_ID, key=["as_of_date", "cusip"])
    publish(DATASET_ID, METADATA)
    save_state("soma_holdings", {"last_date": current_date})


NODES = {
    run: [],
}

if __name__ == "__main__":
    run()
