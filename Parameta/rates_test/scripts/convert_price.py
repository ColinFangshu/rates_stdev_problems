import pandas as pd

class PriceConverter():
    def __init__(self, ccy_df, price_df, spot_rate_df):
        self.ccy_df = ccy_df.copy() 
        self.price_df = price_df.copy()
        self.spot_rate_df = spot_rate_df.copy()
        # Ensure datetime columns
        self.price_df['timestamp'] = pd.to_datetime(self.price_df['timestamp'])
        self.spot_rate_df['timestamp'] = pd.to_datetime(self.spot_rate_df['timestamp'])
        self.price_df = self.price_df.rename(columns={'timestamp': 'price_timestamp'})
        self.spot_rate_df = self.spot_rate_df.rename(columns={'timestamp': 'spot_timestamp'})

    def _merge_conversion_factors(self):
        return self.price_df.merge(
            self.ccy_df[['ccy_pair', 'convert_price', 'conversion_factor']],
            on='ccy_pair',
            how='left'
        )
    
    def _get_latest_spot_rates(self, merged_df):
        # Sort both by the appropriate timestamp
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
    

    def export_to_csv(self, df: pd.DataFrame, filepath: str, index: bool = False) -> None:
        """
        Exports the given DataFrame to a CSV file.

        Args:
            df (pd.DataFrame): The DataFrame to export.
            filepath (str): The full path to the output CSV file.
            index (bool): Whether to include the index in the CSV file. Default is False.
        """
        try:
            df.to_csv(filepath, index=index)
            print(f"✅ File successfully saved to: {filepath}")
        except Exception as e:
            print(f"❌ Failed to export CSV: {e}")


    def calculate_converted_prices(self):
        # Step 1: Merge conversion info
        merged_df = self._merge_conversion_factors()

        # Step 2: Merge in latest spot rate
        enriched = self._get_latest_spot_rates(merged_df)

        # Step 3: Initialize remark and converted_price columns
        enriched['remark'] = ''
        enriched['converted_price'] = enriched['price']  # default

        # Step 4: Flag issues
        enriched = self._flag_conversion_issues(enriched)

        # Step 5: Perform valid conversion
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
            enriched.loc[cond_valid, 'conversion_factor']
            + enriched.loc[cond_valid, 'spot_mid_rate']
        )
        enriched.loc[cond_valid, 'remark'] = 'Converted'

        return enriched[[
            'price_timestamp', 'security_id', 'ccy_pair', 'price', 'conversion_factor',
            'spot_mid_rate', 'converted_price', 'convert_price', 'remark'
        ]]
    

if __name__ == '__main__':
    import pyarrow.parquet as pq

    rates_price_data_table = pq.read_table("Parameta/rates_test/data/rates_price_data.parq.gzip")
    rates_spot_rate_data_table = pq.read_table("Parameta/rates_test/data/rates_spot_rate_data.parq.gzip")

    rates_ccy_data_df = pd.read_csv("Parameta/rates_test/data/rates_ccy_data.csv")

    rates_price_data_df = rates_price_data_table.to_pandas()
    rates_spot_rate_data_df = rates_spot_rate_data_table.to_pandas()

    converter = PriceConverter(rates_ccy_data_df, rates_price_data_df, rates_spot_rate_data_df)
    result_df = converter.calculate_converted_prices()
    print(result_df.head())
    print(result_df[result_df["remark"] == ""])
    
    converter.export_to_csv(result_df, "Parameta/rates_test/results/converted_prices.csv")