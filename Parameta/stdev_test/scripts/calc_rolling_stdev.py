import pandas as pd
import pyarrow.parquet as pq
from typing import Optional
import numpy as np


class RollingPriceStdevCalculator:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.price_df = pq.read_table(file_path).to_pandas()
        self.result_df = None

    def _preprocess(self):
        # Ensures snap_time is datetime, sorts the data
        self.price_df['snap_time'] = pd.to_datetime(self.price_df['snap_time'])
        self.price_df.sort_values(['security_id', 'snap_time'], inplace=True)
        self.price_df.reset_index(drop=True, inplace=True)

    def _get_valid_window_indices(self, group_df: pd.DataFrame, window_size: int = 20):
        valid_end_indices = []
        snap_times = group_df['snap_time'].reset_index(drop=True)

        # Use vectorized diff
        for i in range(len(group_df) - window_size + 1):
            window = snap_times.iloc[i:i + window_size]
            diffs = window.diff().dropna().dt.total_seconds()
            if (diffs == 3600).all():
                valid_end_indices.append(i + window_size - 1)

        return valid_end_indices

    def _calculate_group_stdevs(self, group_df: pd.DataFrame, window_size: int = 20):
        group_df = group_df.copy().reset_index(drop=True)

        # Prepare empty columns
        group_df['bid_stdev'] = np.nan
        group_df['mid_stdev'] = np.nan
        group_df['ask_stdev'] = np.nan

        valid_indices = self._get_valid_window_indices(group_df, window_size)

        for idx in valid_indices:
            window = group_df.iloc[idx - window_size + 1: idx + 1]
            group_df.loc[idx, 'bid_stdev'] = window['bid'].std(ddof=0)
            group_df.loc[idx, 'mid_stdev'] = window['mid'].std(ddof=0)
            group_df.loc[idx, 'ask_stdev'] = window['ask'].std(ddof=0)

        return group_df

    def compute_all(self, start: Optional[str] = None, end: Optional[str] = None, window_size: int = 20):
        self._preprocess()

        result = []
        for sec_id, group_df in self.price_df.groupby('security_id'):
            calculated = self._calculate_group_stdevs(group_df, window_size)
            result.append(calculated)

        all_df = pd.concat(result, ignore_index=True)

        if start and end:
            all_df = all_df[
                (all_df['snap_time'] >= pd.to_datetime(start)) &
                (all_df['snap_time'] <= pd.to_datetime(end))
            ]

        self.result_df = all_df

    def save_to_csv(self, output_path: str):
        if self.result_df is None:
            raise ValueError("No results found. Run compute_all() first.")
        self.result_df.to_csv(output_path, index=False)


if __name__ == '__main__':
    price_stdev_calculator = RollingPriceStdevCalculator(file_path="Parameta/stdev_test/data/stdev_price_data.parq.gzip")

    price_stdev_calculator.compute_all(start="2021-11-20 00:00:00", end="2021-11-23 09:00:00")
    price_stdev_calculator.save_to_csv("Parameta/stdev_test/results/rolling_stdev_output.csv")