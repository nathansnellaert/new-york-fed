import os
import json
from pathlib import Path
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from assets.reference_rates.reference_rates import process_reference_rates
from assets.soma_holdings.soma_holdings import process_soma_holdings
from assets.primary_dealer_stats.primary_dealer_stats import process_primary_dealer_stats
from assets.ambs_operations.ambs_operations import process_ambs_operations
from assets.treasury_operations.treasury_operations import process_treasury_operations
from assets.securities_lending.securities_lending import process_securities_lending
from assets.fx_swaps.fx_swaps import process_fx_swaps
from assets.repo_operations.repo_operations import process_repo_operations

def validate_environment():
    """Validate required environment variables exist"""
    required = ["R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ENDPOINT_URL", "R2_BUCKET_NAME", "GITHUB_RUN_ID"]
    missing = [var for var in required if var not in os.environ]
    if missing:
        raise ValueError(f"Missing environment variables: {missing}")

def upload_to_r2(data, dataset_name):
    """Upload data to Cloudflare R2 as Parquet"""
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        endpoint_url=os.environ["R2_ENDPOINT_URL"]
    )
    
    buffer = pa.BufferOutputStream()
    pq.write_table(data, buffer)
    
    key = f"{dataset_name}/{os.environ['GITHUB_RUN_ID']}.parquet"
    s3_client.put_object(
        Bucket=os.environ["R2_BUCKET_NAME"],
        Key=key,
        Body=buffer.getvalue().to_pybytes()
    )

def main():
    validate_environment()
    
    # Process assets (returns only new/changed data)
    reference_rates_data = process_reference_rates()
    soma_holdings_data = process_soma_holdings()
    primary_dealer_data = process_primary_dealer_stats()
    ambs_data = process_ambs_operations()
    treasury_data = process_treasury_operations()
    seclending_data = process_securities_lending()
    fx_swaps_data = process_fx_swaps()
    repo_data = process_repo_operations()
    
    # Upload data to R2
    upload_to_r2(reference_rates_data, "new-york-fed/reference_rates")
    upload_to_r2(soma_holdings_data, "new-york-fed/soma_holdings")
    upload_to_r2(primary_dealer_data, "new-york-fed/primary_dealer_stats")
    upload_to_r2(ambs_data, "new-york-fed/ambs_operations")
    upload_to_r2(treasury_data, "new-york-fed/treasury_operations")
    upload_to_r2(seclending_data, "new-york-fed/securities_lending")
    upload_to_r2(fx_swaps_data, "new-york-fed/fx_swaps")
    upload_to_r2(repo_data, "new-york-fed/repo_operations")

if __name__ == "__main__":
    main()