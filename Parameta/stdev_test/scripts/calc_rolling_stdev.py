import pandas as pd
import pyarrow.parquet as pq
from typing import Optional, List
import numpy as np
import time
import os
import psutil


class RollingPriceStdevCalculator:
    """
    Calculates rolling standard deviation for bid, mid, ask prices per security ID based on 20 contiguous hourly snaps.
    """
    def __init__(self, file_path: str):
        """Initializes the calculator and loads data from a Parquet file."""
        self.file_path = file_path
        self.price_df = pq.read_table(file_path).to_pandas()
        self.result_df = None

    def _preprocess(self) -> None:
        """Converts timestamps and sorts the dataframe by security_id and snap_time."""
        self.price_df['snap_time'] = pd.to_datetime(self.price_df['snap_time'])
        self.price_df.sort_values(['security_id', 'snap_time'], inplace=True)
        self.price_df.reset_index(drop=True, inplace=True)

    def _get_valid_window_indices(self, group_df: pd.DataFrame, window_size: int = 20) -> List[int]:
        """Returns list of valid end indices where timestamps are contiguous for rolling window."""        
        valid_end_indices = []
        snap_times = group_df['snap_time'].reset_index(drop=True)

        # Use vectorized diff
        for i in range(len(group_df) - window_size + 1):
            window = snap_times.iloc[i:i + window_size]
            diffs = window.diff().dropna().dt.total_seconds()
            if (diffs == 3600).all():
                valid_end_indices.append(i + window_size - 1)

        return valid_end_indices

    def _calculate_group_stdevs(self, group_df: pd.DataFrame, window_size: int = 20) -> pd.DataFrame:
        """
        Calculates rolling standard deviation for bid, mid, and ask over a specified window size.

        The function performs a fresh calculation for each group without relying on any previous intermediate results.
        It computes the full rolling standard deviation first, then masks out rows where the preceding timestamps are
        not strictly contiguous (i.e., 19 consecutive 1-hour gaps). This ensures only valid 20-hour windows are retained.
        """
        group_df = group_df.copy().reset_index(drop=True)

        # Step 1: Compute time_diff in seconds
        group_df['time_diff'] = group_df['snap_time'].diff().dt.total_seconds()

        # Step 2: Use rolling to check if last 19 time_diff values == 3600 (i.e., 1-hour gaps)
        is_contiguous = (
            group_df['time_diff']
            .rolling(window=window_size - 1)
            .apply(lambda x: np.all(x == 3600), raw=True)
            .shift(1)  # align with current row
        )

        # Step 3: Apply rolling std
        group_df['bid_stdev'] = group_df['bid'].rolling(window=window_size).std(ddof=0)
        group_df['mid_stdev'] = group_df['mid'].rolling(window=window_size).std(ddof=0)
        group_df['ask_stdev'] = group_df['ask'].rolling(window=window_size).std(ddof=0)

        # Step 4: Invalidate stdevs where window isn't contiguous
        invalid_mask = is_contiguous != 1.0
        group_df.loc[invalid_mask, ['bid_stdev', 'mid_stdev', 'ask_stdev']] = np.nan

        # Optional cleanup
        group_df.drop(columns='time_diff', inplace=True)

        return group_df

    def compute_all(self, start: Optional[str] = None, end: Optional[str] = None, window_size: int = 20) -> None:
        """Runs the full rolling stdev calculation for all security IDs."""
        print("Preprocessing data...")
        self._preprocess()

        result = []
        print("Processing security_id groups...")
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
        print("Rolling standard deviation calculation complete.")

    def save_to_csv(self, output_path: str) -> None:
        """Saves the result DataFrame to a CSV file."""
        if self.result_df is None:
            raise ValueError("No results found. Run compute_all() first.")
        self.result_df.to_csv(output_path, index=False)
        print(f"Results saved to: {output_path}")

def log_performance(calculator: RollingPriceStdevCalculator, output_path: str, start_time: float) -> None:
    """Logs runtime, memory usage, and result summary for the current run."""
    process = psutil.Process(os.getpid())
    mem_used = process.memory_info().rss / 1e6  # MB
    elapsed = time.time() - start_time
    result_df = calculator.result_df

    print("\n=== PERFORMANCE REPORT ===")
    print(f"Time taken: {elapsed:.2f} seconds")
    print(f"Memory used: {mem_used:.2f} MB")
    print(f"Output file exists: {os.path.exists(output_path)}")
    print(f"Output shape: {result_df.shape}")
    print(f"Unique security_ids: {result_df['security_id'].nunique()}")
    print(f"Non-null bid_stdev: {result_df['bid_stdev'].notna().sum()}")
    print(f"Non-null mid_stdev: {result_df['mid_stdev'].notna().sum()}")
    print(f"Non-null ask_stdev: {result_df['ask_stdev'].notna().sum()}")
    print("==========================\n")


if __name__ == '__main__':
    start_time = time.time()
    print("Starting rolling stdev app...")

    price_stdev_calculator = RollingPriceStdevCalculator(
        file_path="Parameta/stdev_test/data/stdev_price_data.parq.gzip"
    )

    price_stdev_calculator.compute_all(
        start="2021-11-20 00:00:00",
        end="2021-11-23 09:00:00"
    )

    output_csv = "Parameta/stdev_test/results/rolling_stdev_output.csv"
    price_stdev_calculator.save_to_csv(output_csv)

    log_performance(price_stdev_calculator, output_csv, start_time)

    print("Done.")