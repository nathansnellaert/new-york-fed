"""Transform New York Fed Treasury operations data."""

import pyarrow as pa
from subsets_utils import load_raw_json, upload_data, publish
from utils import parse_date, parse_number
from .test import test

DATASET_ID = "nyf_treasury_operations"

METADATA = {
    "id": DATASET_ID,
    "title": "New York Fed Treasury Securities Operations",
    "description": "Treasury securities purchase and sale operations conducted by the New York Fed Open Market Trading Desk.",
    "column_descriptions": {
        "operation_date": "Date of the operation",
        "operation_id": "Unique operation identifier",
        "operation_type": "Type of operation",
        "operation_direction": "Direction of operation (buy/sell)",
        "settlement_date": "Settlement date",
        "cusip": "CUSIP identifier",
        "security_description": "Description of the security",
        "maturity_date_start": "Start of maturity range",
        "maturity_date_end": "End of maturity range",
        "auction_method": "Auction method used",
        "par_amount_submitted": "Total par amount submitted",
        "par_amount_accepted": "Par amount accepted",
        "weighted_avg_price": "Weighted average accepted price",
        "least_favorable_price": "Least favorable accepted price",
        "release_time": "Time results were released",
        "close_time": "Time operation closed",
    }
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


def run():
    """Transform Treasury operations raw data and upload."""
    raw_data = load_raw_json("treasury_operations")

    auctions = raw_data.get("auctions", [])
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

    print(f"  Transformed {len(records):,} Treasury operation records")
    table = pa.Table.from_pylist(records, schema=SCHEMA)

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
