# new-york-fed

Federal Reserve Bank of New York market data connector.

## Datasets

| Dataset | Description | Key |
|---------|-------------|-----|
| `nyf_reference_rates` | Daily reference rates (SOFR, EFFR, OBFR, BGCR, TGCR) | `rate_type, date` |
| `nyf_soma_holdings` | SOMA holdings of Treasury and agency securities | `as_of_date, cusip` |
| `nyf_primary_dealer_stats` | Weekly primary dealer positions and financing | `week_ending, series_code` |
| `nyf_ambs_operations` | Agency MBS purchase/sale operations | `operation_id` |
| `nyf_treasury_operations` | Treasury purchase/sale operations | `operation_id, cusip` |
| `nyf_securities_lending` | Securities lending operations from SOMA | `operation_id, cusip` |
| `nyf_fx_swaps` | FX swap operations with central banks | `trade_date, settlement_date, maturity_date, currency` |
| `nyf_repo_operations` | Repo and reverse repo operations | `operation_id, security_type` |

## Coverage

All datasets come from the NY Fed Markets API (`markets.newyorkfed.org/api`). Reference rates start from April 2018 (SOFR inception). All other datasets start from January 2020. All datasets support incremental updates via state tracking.

## Schedule

Runs daily at 2 PM UTC (9 AM ET), after the Fed publishes its daily data.
