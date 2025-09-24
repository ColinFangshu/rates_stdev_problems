import pandas as pd
import pyarrow.parquet as pq
import numpy as np
from typing import Optional
import os
import psutil
import time


class PriceConverter:
    """Handles price conversion logic including data loading, spot merging, validation, and export."""

    def __init__(self, price_path: str, spot_path: str, ccy_path: str):
        """Initializes and loads all input files from provided paths."""
        self.ccy_df = pd.read_csv(ccy_path)
        self.price_df = pq.read_table(price_path).to_pandas()
        self.spot_rate_df = pq.read_table(spot_path).to_pandas()

        # Standardize and convert timestamps
        self.price_df['timestamp'] = pd.to_datetime(self.price_df['timestamp'])
        self.spot_rate_df['timestamp'] = pd.to_datetime(self.spot_rate_df['timestamp'])
        self.price_df = self.price_df.rename(columns={'timestamp': 'price_timestamp'})
        self.spot_rate_df = self.spot_rate_df.rename(columns={'timestamp': 'spot_timestamp'})

        self.result_df: Optional[pd.DataFrame] = None

    def _merge_conversion_factors(self) -> pd.DataFrame:
        """Joins price_df with ccy_df to bring in conversion metadata."""
        return self.price_df.merge(
            self.ccy_df[['ccy_pair', 'convert_price', 'conversion_factor']],
            on='ccy_pair',
            how='left'
        )
    
    def _get_latest_spot_rates(self, merged_df: pd.DataFrame) -> pd.DataFrame:
        """Performs merge_asof to attach latest spot rate for each price timestamp."""
        merged_sorted = merged_df.sort_values('price_timestamp')
        spot_sorted = self.spot_rate_df.sort_values('spot_timestamp')

        # Perform merge_asof with left_on and right_on
        enriched = pd.merge_asof(
            merged_sorted,
            spot_sorted,
            left_on='price_timestamp',
            right_on='spot_timestamp',
            by='ccy_pair',
            direction='backward'
        )
        return enriched
    
    def _flag_conversion_issues(self, enriched: pd.DataFrame) -> pd.DataFrame:
        """Assigns appropriate remark flags based on conversion logic rules."""

        # Step 1: Flag missing price
        enriched.loc[enriched['price'].isnull(), 'remark'] = 'Missing price'

        # Step 2: Flag ccy_pair not found in ccy_df
        cond_not_found = (
            (enriched['remark'] == '') &
            (enriched['convert_price'].isnull()) &
            (enriched['conversion_factor'].isnull()) &
            (enriched['ccy_pair'].notnull())
        )
        enriched.loc[cond_not_found, 'remark'] = 'ccy_pair not found in ccy_df'

        # Step 3: Flag missing or invalid ccy_pair
        enriched.loc[
            enriched['ccy_pair'].isnull() | (enriched['ccy_pair'].str.strip() == ''),
            'remark'
        ] = 'Missing or invalid ccy_pair'

        # Step 4: Flag missing conversion factor when required
        cond_cf_missing = (
            (enriched['convert_price'] == True) &
            (enriched['conversion_factor'].isnull()) &
            (enriched['remark'] == '')
        )
        enriched.loc[cond_cf_missing, 'remark'] = 'Missing conversion_factor'

        # Step 5: Flag conversion not required
        cond_cf_false = (
            (enriched['convert_price'] == False) &
            (enriched['remark'] == '')
        )
        enriched.loc[cond_cf_false, 'remark'] = 'Conversion not required'

        # Step 6: Flag missing spot rate when all else is valid
        cond_spot_missing = (
            (enriched['convert_price'] == True) &
            (enriched['conversion_factor'].notnull()) &
            (enriched['spot_mid_rate'].isnull()) &
            (enriched['remark'] == '')
        )
        enriched.loc[cond_spot_missing, 'remark'] = 'Missing spot_mid_rate'

        # Step 7: Flag spot rate too old
        time_diff = enriched['price_timestamp'] - enriched['spot_timestamp']
        cond_spot_out_of_window = (
            (enriched['remark'] == '') &
            (enriched['convert_price'] == True) &
            (enriched['conversion_factor'].notnull()) &
            (enriched['spot_mid_rate'].notnull()) &
            (time_diff > pd.Timedelta(hours=1))
        )
        enriched.loc[cond_spot_out_of_window, 'remark'] = 'Spot rate too old (>1hr)'

        return enriched

    def calculate_converted_prices(self) -> None:
        """Executes the full conversion logic and stores the enriched result internally."""
        print("  Step 1: Merging conversion factors...")
        merged_df = self._merge_conversion_factors()

        print("  Step 2: Attaching latest spot rates...")
        enriched = self._get_latest_spot_rates(merged_df)

        print("  Step 3: Initializing columns...")
        enriched['remark'] = ''
        enriched['converted_price'] = enriched['price']

        print("  Step 4: Flagging conversion issues...")
        enriched = self._flag_conversion_issues(enriched)

        print("  Step 5: Calculating valid converted prices...")
        time_diff = enriched['price_timestamp'] - enriched['spot_timestamp']
        cond_valid = (
            (enriched['remark'] == '') &
            (enriched['convert_price'] == True) &
            (enriched['conversion_factor'].notnull()) &
            (enriched['spot_mid_rate'].notnull()) &
            (time_diff.between(pd.Timedelta(0), pd.Timedelta(hours=1)))
        )

        enriched.loc[cond_valid, 'converted_price'] = (
            enriched.loc[cond_valid, 'price'] / 
            enriched.loc[cond_valid, 'conversion_factor'] +
            enriched.loc[cond_valid, 'spot_mid_rate']
        )
        enriched.loc[cond_valid, 'remark'] = 'Converted'

        self.result_df = enriched[[
            'price_timestamp', 'security_id', 'ccy_pair', 'price', 'conversion_factor',
            'spot_mid_rate', 'converted_price', 'convert_price', 'remark'
        ]]
    
    def save_to_csv(self, output_path: str) -> None:
        """Saves the computed result DataFrame to a CSV file."""
        if self.result_df is None:
            raise ValueError("No results to save. Run calculate_converted_prices() first.")
        self.result_df.to_csv(output_path, index=False)
        print(f"Result saved to: {output_path}")
    
def log_performance(converter: PriceConverter, output_path: str, start_time: float) -> None:
    """Logs memory usage, time elapsed, and key metrics after conversion run."""
    process = psutil.Process(os.getpid())
    mem_used = process.memory_info().rss / 1e6
    elapsed = time.time() - start_time
    df = converter.result_df

    print("\n=== PERFORMANCE REPORT ===")
    print(f"  Time taken: {elapsed:.2f} seconds")
    print(f"  Memory used: {mem_used:.2f} MB")
    print(f"  Output path: {output_path}")
    print(f"  Output exists: {os.path.exists(output_path)}")
    print(f"  Output shape: {df.shape}")
    print(f"  Empty remarks: {(df['remark'] == '').sum()}")
    print(f"  Converted rows: {(df['remark'] == 'Converted').sum()}")
    print("==========================\n")


if __name__ == '__main__':
    start_time = time.time()

    print("Initializing PriceConverter...")
    converter = PriceConverter(
        price_path="Parameta/rates_test/data/rates_price_data.parq.gzip",
        spot_path="Parameta/rates_test/data/rates_spot_rate_data.parq.gzip",
        ccy_path="Parameta/rates_test/data/rates_ccy_data.csv"
    )

    print("Running conversion pipeline...")
    converter.calculate_converted_prices()

    output_csv = "Parameta/rates_test/results/converted_prices.csv"
    converter.save_to_csv(output_csv)

    log_performance(converter, output_csv, start_time)