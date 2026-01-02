import os
import argparse

os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from subsets_utils import validate_environment

# Ingest modules
from ingest import reference_rates as ingest_reference_rates
from ingest import soma as ingest_soma
from ingest import primary_dealer as ingest_primary_dealer
from ingest import ambs as ingest_ambs
from ingest import treasury as ingest_treasury
from ingest import seclending as ingest_seclending
from ingest import fx_swaps as ingest_fx_swaps
from ingest import repo as ingest_repo

# Transform modules
from transforms import reference_rates as transform_reference_rates
from transforms import soma_holdings as transform_soma_holdings
from transforms import primary_dealer_stats as transform_primary_dealer_stats
from transforms import ambs_operations as transform_ambs_operations
from transforms import treasury_operations as transform_treasury_operations
from transforms import securities_lending as transform_securities_lending
from transforms import fx_swaps as transform_fx_swaps
from transforms import repo_operations as transform_repo_operations


def main():
    parser = argparse.ArgumentParser(description="NY Fed Data Connector")
    parser.add_argument("--ingest-only", action="store_true", help="Only fetch data from API")
    parser.add_argument("--transform-only", action="store_true", help="Only transform existing raw data")
    args = parser.parse_args()

    validate_environment()

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")

        print("\n--- Reference Rates ---")
        ingest_reference_rates.run()

        print("\n--- SOMA Holdings ---")
        ingest_soma.run()

        print("\n--- Primary Dealer Stats ---")
        ingest_primary_dealer.run()

        print("\n--- AMBS Operations ---")
        ingest_ambs.run()

        print("\n--- Treasury Operations ---")
        ingest_treasury.run()

        print("\n--- Securities Lending ---")
        ingest_seclending.run()

        print("\n--- FX Swaps ---")
        ingest_fx_swaps.run()

        print("\n--- Repo Operations ---")
        ingest_repo.run()

    if should_transform:
        print("\n=== Phase 2: Transform ===")

        print("\n--- Reference Rates ---")
        transform_reference_rates.run()

        print("\n--- SOMA Holdings ---")
        transform_soma_holdings.run()

        print("\n--- Primary Dealer Stats ---")
        transform_primary_dealer_stats.run()

        print("\n--- AMBS Operations ---")
        transform_ambs_operations.run()

        print("\n--- Treasury Operations ---")
        transform_treasury_operations.run()

        print("\n--- Securities Lending ---")
        transform_securities_lending.run()

        print("\n--- FX Swaps ---")
        transform_fx_swaps.run()

        print("\n--- Repo Operations ---")
        transform_repo_operations.run()


if __name__ == "__main__":
    main()
