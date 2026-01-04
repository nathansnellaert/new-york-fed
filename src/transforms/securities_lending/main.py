"""Transform New York Fed securities lending data."""

from datetime import datetime
import pyarrow as pa
from subsets_utils import load_raw_json, upload_data, publish
from .test import test

DATASET_ID = "nyf_securities_lending"

METADATA = {
    "id": DATASET_ID,
    "title": "New York Fed Securities Lending Operations",
    "description": "Securities lending operations from the New York Fed's SOMA portfolio, providing liquidity in Treasury securities markets.",
    "column_descriptions": {
        "operation_date": "Date of the operation",
        "operation_id": "Unique operation identifier",
        "settlement_date": "Settlement date",
        "maturity_date": "Maturity date of the loan",
        "cusip": "CUSIP identifier of the security",
        "security_description": "Description of the security",
        "par_amount_submitted": "Par amount submitted by dealers",
        "par_amount_accepted": "Par amount accepted",
        "weighted_average_rate": "Weighted average lending rate",
        "soma_holdings": "SOMA holdings of the security",
        "theoretical_available": "Theoretical amount available to borrow",
        "actual_available": "Actual amount available to borrow",
        "outstanding_loans": "Outstanding loan amount",
        "release_time": "Time results were released",
        "close_time": "Time operation closed",
    }
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
    """Transform Securities Lending operations raw data and upload."""
    raw_data = load_raw_json("securities_lending")

    operations = raw_data.get("operations", [])
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

    print(f"  Transformed {len(records):,} Securities Lending records")
    table = pa.Table.from_pylist(records, schema=SCHEMA)

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
