import pandas as pd

class PriceConverter():
    def __init__(self, ccy_df, price_df, spot_rate_df):
        self.ccy_df = ccy_df.copy() 
        self.price_df = price_df.copy()
        self.spot_rate_df = spot_rate_df.copy()
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
    
    def calculate_converted_prices(self):
        # Step 1: Merge in conversion factor
        merged_df = self._merge_conversion_factors()

        # Step 2: Merge in latest spot rate
        enriched_df = self._get_latest_spot_rates(merged_df)

        # Step 3: Calculate new price
        # Default to raw price
        enriched_df['converted_price'] = enriched_df['price']

        # Apply formula only where convert_price is True and required fields are not null
        # and the spot_rate timestamp should be within the hour that precedes the price timestamp. 
        condition = (
            (enriched_df['convert_price'] == True) &
            (enriched_df['conversion_factor'].notnull()) &
            (enriched_df['spot_mid_rate'].notnull()) &
            (enriched_df['price_timestamp'] - enriched_df['spot_timestamp']).between(
                pd.Timedelta(0), pd.Timedelta(hours=1)
            )
        )

        enriched_df.loc[condition, 'converted_price'] = (
            (enriched_df.loc[condition, 'price'] / enriched_df.loc[condition, 'conversion_factor'])
            + enriched_df.loc[condition, 'spot_mid_rate']
        )

        return enriched_df[['price_timestamp', 'security_id', 'ccy_pair', 'price', 'converted_price']]
    

if __name__ == '__main__':
    import pyarrow.parquet as pq

    rates_price_data_table = pq.read_table("../data/rates_price_data.parq.gzip")
    rates_spot_rate_data_table = pq.read_table("../data/rates_spot_rate_data.parq.gzip")

    rates_ccy_data_df = pd.read_csv("../data/rates_ccy_data.csv")
    
    rates_price_data_df = rates_price_data_table.to_pandas()
    rates_spot_rate_data_df = rates_spot_rate_data_table.to_pandas()

    converter = PriceConverter(rates_ccy_data_df, rates_price_data_df, rates_spot_rate_data_df)
    result_df = converter.calculate_converted_prices()
    print(result_df.head())