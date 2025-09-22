import pandas as pd

class PriceConverter():
    def __init__(self):
        pass

    def _merge_conversion_factors(self):
        return merged
    
    def _get_latest_spot_rates(self):
        return enriched
    
    def calculated_converted_prices(self):
        # Step 1: Merge conversion info
        merged = self._merge_conversion_factors()

        # Step 2: Merge spot rate
        enriched = self._get_latest_spot_rates(merged)

        # Step 3: Calculate new price
        # Default to raw price
        enriched['converted_price'] = enriched['price']

        # Apply formula only where convert_price is True and required fields are not null
        # and the spot_rate timestamp should be within the hour that precedes the price timestamp. 
        condition = (
            (enriched['convert_price'] == True) &
            (enriched['conversion_factor'].notnull()) &
            (enriched['spot_mid_rate'].notnull())
        )

        return enriched[['timestamp', 'security_id', 'ccy_pair', 'price', 'converted_price']]