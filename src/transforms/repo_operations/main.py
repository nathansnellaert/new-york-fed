"""Transform New York Fed repo operations data."""

import pyarrow as pa
from subsets_utils import load_raw_json, upload_data, publish
from utils import parse_date, parse_number, parse_integer
from .test import test

DATASET_ID = "nyf_repo_operations"

METADATA = {
    "id": DATASET_ID,
    "title": "New York Fed Repo Operations",
    "description": "Repurchase agreement (repo) and reverse repo operations conducted by the New York Fed Open Market Trading Desk.",
    "column_descriptions": {
        "operation_date": "Date of the operation",
        "operation_id": "Unique operation identifier",
        "operation_type": "Type of operation (repo, reverse repo)",
        "operation_method": "Method used for operation",
        "settlement_date": "Settlement date",
        "maturity_date": "Maturity date of the operation",
        "term": "Term description",
        "term_calendar_days": "Term length in calendar days",
        "settlement_type": "Type of settlement",
        "security_type": "Type of security accepted",
        "amount_submitted": "Amount submitted by counterparties",
        "amount_accepted": "Amount accepted by Fed",
        "total_amount_submitted": "Total amount submitted across all security types",
        "total_amount_accepted": "Total amount accepted across all security types",
        "participating_counterparties": "Number of participating counterparties",
        "accepted_counterparties": "Number of accepted counterparties",
        "offering_rate": "Offered rate (percent)",
        "award_rate": "Award rate (percent)",
        "weighted_average_rate": "Weighted average rate (percent)",
        "minimum_bid_rate": "Minimum bid rate",
        "maximum_bid_rate": "Maximum bid rate",
        "release_time": "Time results were released",
        "close_time": "Time operation closed",
    }
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


def run():
    """Transform repo operations raw data and upload."""
    raw_data = load_raw_json("repo_operations")

    operations = raw_data.get("operations", [])
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

    print(f"  Transformed {len(records):,} repo operation records")
    table = pa.Table.from_pylist(records, schema=SCHEMA)

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
