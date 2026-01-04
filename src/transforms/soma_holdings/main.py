"""Transform New York Fed SOMA holdings data."""

from datetime import datetime
import pyarrow as pa
from subsets_utils import load_raw_json, upload_data, publish
from .test import test

DATASET_ID = "nyf_soma_holdings"

METADATA = {
    "id": DATASET_ID,
    "title": "New York Fed SOMA Holdings",
    "description": "System Open Market Account (SOMA) holdings of Treasury and Agency securities held by the Federal Reserve.",
    "column_descriptions": {
        "as_of_date": "Date of holdings snapshot",
        "security_type": "Type of security (Treasury Bill, Note, Bond, Agency)",
        "cusip": "CUSIP identifier",
        "security_description": "Description of the security",
        "maturity_date": "Maturity date of the security",
        "issuer": "Security issuer",
        "coupon_rate": "Coupon rate (percent)",
        "par_value": "Par value held",
        "percent_outstanding": "Percent of total outstanding held by SOMA",
        "change_from_prior_week": "Change in holdings from prior week",
        "change_from_prior_year": "Change in holdings from prior year",
    }
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


def run():
    """Transform SOMA holdings raw data and upload."""
    raw_data = load_raw_json("soma_holdings")

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

    print(f"  Transformed {len(records):,} SOMA holdings records")
    table = pa.Table.from_pylist(records, schema=SCHEMA)

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
