import os
os.environ['CONNECTOR_NAME'] = 'nyf'
os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from utils import validate_environment, upload_data
from assets.reference_rates.reference_rates import process_reference_rates
from assets.soma_holdings.soma_holdings import process_soma_holdings
from assets.primary_dealer_stats.primary_dealer_stats import process_primary_dealer_stats
from assets.ambs_operations.ambs_operations import process_ambs_operations
from assets.treasury_operations.treasury_operations import process_treasury_operations
from assets.securities_lending.securities_lending import process_securities_lending
from assets.fx_swaps.fx_swaps import process_fx_swaps
from assets.repo_operations.repo_operations import process_repo_operations

def main():
    validate_environment()
    
    # Process and upload each asset immediately
    reference_rates_data = process_reference_rates()
    upload_data(reference_rates_data, "nyf_reference_rates")
    
    soma_holdings_data = process_soma_holdings()
    upload_data(soma_holdings_data, "nyf_soma_holdings")
    
    primary_dealer_data = process_primary_dealer_stats()
    upload_data(primary_dealer_data, "nyf_primary_dealer_stats")
    
    ambs_data = process_ambs_operations()
    upload_data(ambs_data, "nyf_ambs_operations")
    
    treasury_data = process_treasury_operations()
    upload_data(treasury_data, "nyf_treasury_operations")
    
    seclending_data = process_securities_lending()
    upload_data(seclending_data, "nyf_securities_lending")
    
    fx_swaps_data = process_fx_swaps()
    upload_data(fx_swaps_data, "nyf_fx_swaps")
    
    repo_data = process_repo_operations()
    upload_data(repo_data, "nyf_repo_operations")

if __name__ == "__main__":
    main()