"""Transform New York Fed AMBS operations data."""

from datetime import datetime
import pyarrow as pa
from subsets_utils import load_raw_json, upload_data, publish
from .test import test

DATASET_ID = "nyf_ambs_operations"

METADATA = {
    "id": DATASET_ID,
    "title": "New York Fed Agency MBS Operations",
    "description": "Agency mortgage-backed securities (MBS) operations conducted by the New York Fed Open Market Trading Desk.",
    "column_descriptions": {
        "operation_date": "Date of the operation",
        "operation_id": "Unique operation identifier",
        "operation_type": "Type of MBS operation",
        "operation_direction": "Direction of operation (buy/sell)",
        "settlement_date": "Settlement date",
        "security_description": "Description of the MBS security",
        "class_type": "MBS class type",
        "method": "Auction method",
        "amount_submitted_par": "Total par amount submitted",
        "amount_accepted_par": "Par amount accepted",
        "release_time": "Time results were released",
        "close_time": "Time operation closed",
        "inclusion_flag": "Inclusion/exclusion flag",
    }
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


def run():
    """Transform AMBS operations raw data and upload."""
    raw_data = load_raw_json("ambs_operations")

    auctions = raw_data.get("auctions", [])
    records = []

    for auction in auctions:
        if auction.get("auctionStatus") != "Results":
            continue

        operation_date = parse_date(auction.get("operationDate"))
        details = auction.get("details", [])

        if details:
            for detail in details:
                records.append({
                    "operation_date": operation_date,
                    "operation_id": auction.get("operationId"),
                    "operation_type": auction.get("operationType"),
                    "operation_direction": auction.get("operationDirection"),
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

    print(f"  Transformed {len(records):,} AMBS operation records")
    table = pa.Table.from_pylist(records, schema=SCHEMA)

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
