import pandas as pd
import pyarrow.parquet as pq
from typing import Optional, List, Dict
import numpy as np
from collections import deque
import math
import time
import os
import psutil


class RollingPriceStdevCalculator:
    """
    Rolling stdev for bid/mid/ask per security_id using the most recent block
    of 20 contiguous hourly snaps strictly before each snap_time.

    Rules implemented:
      - Work on a complete hourly calendar per security in the [start, end] range.
      - Missing snaps remain as rows (prices NaN), but we still compute stdev
        at those times using the last contiguous 20 valid snaps that occurred earlier.
      - A window never includes the current row's value.
      - If < 20 contiguous valid values exist yet, result is NaN.
    """
    def __init__(self, file_path: str):
        """Initializes the calculator and loads data from a Parquet file."""
        self.file_path = file_path
        self.price_df = pq.read_table(file_path).to_pandas()
        self.result_df = None

    @staticmethod
    def _hourly_index(start: pd.Timestamp, end: pd.Timestamp) -> pd.DatetimeIndex:
        return pd.date_range(start=start, end=end, freq="h")

    def _preprocess(self) -> None:
        """Converts timestamps and sorts the dataframe by security_id and snap_time."""
        self.price_df['snap_time'] = pd.to_datetime(self.price_df['snap_time'])
        self.price_df.sort_values(['security_id', 'snap_time'], inplace=True)
        self.price_df.reset_index(drop=True, inplace=True)

    def _expand_to_full_grid(self, start, end) -> pd.DataFrame:
        """Reindex each security_id to a full hourly calendar. Keeps original values; missing snaps become NaN rows."""
        start_ts = pd.to_datetime(start)
        end_ts = pd.to_datetime(end)

        full_idx = self._hourly_index(start_ts, end_ts)

        out = []
        for sec_id, g in self.price_df.groupby('security_id', sort=False):
            g = g.set_index('snap_time').sort_index() # assume snap_time contains no duplicates 
            g = g.reindex(full_idx)  # adds missing hours
            g['security_id'] = sec_id
            g.index.name = 'snap_time'
            g.reset_index(inplace=True)
            out.append(g)

        return pd.concat(out, ignore_index=True)

    @staticmethod
    def _rolling_stdev_with_deques(group_df, cols, window=20, eps=1e-8) -> pd.DataFrame: 
        """
        Single pass over time for one security_id.
        For each column, maintain a deque of the most recent contiguous valid values.
        Compute stdev at time t using the deque before considering row t's value.
        """
        n = len(group_df)
        times = group_df['snap_time'].to_numpy()
        outputs = {f"{c}_stdev": np.full(n, np.nan, dtype=float) for c in cols}

        dq = {c: deque() for c in cols}
        vals = {c: group_df[c].to_numpy() for c in cols}

        for i in range(n):
            t = times[i]
            # 1) compute outputs from the current deque (exclude row i)
            for c in cols:
                if len(dq[c]) >= window:
                    arr = np.array(dq[c], dtype=np.float64)
                    mu = arr.mean()
                    var = ((arr - mu) ** 2).mean()
                    stdev = math.sqrt(var)
                    outputs[f"{c}_stdev"][i] = 0.0 if stdev < eps else stdev # zero out tiny results
            
            # 2) update deques using *current* row (for next timestamp's compute)
            for c in cols:
                v = vals[c][i]
                if not np.isnan(v):
                    dq[c].append(v)
                    if len(dq[c]) > window:
                        dq[c].popleft()

        out_df = group_df[['security_id', 'snap_time']].copy()
        for name, arr in outputs.items():
            out_df[name] = arr
        return out_df

    def compute_all(self, start, end, window_size = 20) -> None:
        """
        Full pipeline:
          1) preprocess raw dataframe
          2) build complete hourly grid per security_id in [start, end]
          3) per security, compute stdev for bid/mid/ask with deques
        """
        self._preprocess()
        print("Building hourly calendar and filling gaps...")
        full = self._expand_to_full_grid(start, end)

        print("Computing rolling stdevs with contiguous windows (deque)...")
        results = []
        for sec_id, g in full.groupby('security_id', sort=False):
            g = g.sort_values('snap_time').reset_index(drop=True)
            out = self._rolling_stdev_with_deques(
                g, cols=['bid', 'mid', 'ask'], window=window_size
            )
            results.append(
                g.merge(out, on=['security_id', 'snap_time'], how='left')
            )

        self.result_df = pd.concat(results, ignore_index=True)

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