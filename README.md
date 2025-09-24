# RATES_STDEV_PROBLEMS

This project includes two Python scripts to process hourly price data for securities.

## 🔧 Problem Overview

Both problems arise from applications that generate hourly price snaps for a set of security IDs.

1. **Price Conversion**
   - Assigns the latest valid spot rate to each price entry based on `ccy_pair`.
   - Applies conversion using metadata (conversion factor, convert flag).
   - Flags various issues: missing prices, invalid ccy_pair, stale spot rates, etc.

2. **Rolling Standard Deviation**
   - Calculates rolling standard deviation for `bid`, `mid`, and `ask` columns.
   - Only uses 20-hour windows with strictly contiguous 1-hour intervals.
   - Ensures efficient processing and flags invalid snap gaps.

## 📁 Folder Structure

```
Parameta/
├── rates_test/
│   ├── data/          # Input files: price, spot, ccy
│   ├── results/       # Output: converted_prices.csv
│   └── scripts/       # Script: convert_price.py
├── stdev_test/
│   ├── data/          # Input: stdev_price_data.parq.gzip
│   ├── results/       # Output: rolling_stdev_output.csv
│   └── scripts/       # Script: calc_rolling_stdev.py
```

## ▶️ How to Run

### Price Conversion

```bash
python Parameta/rates_test/scripts/convert_price.py
```
Output → `Parameta/rates_test/results/converted_prices.csv`

### Rolling StdDev

```bash
python Parameta/stdev_test/scripts/calc_rolling_stdev.py
```
Output → `Parameta/stdev_test/results/rolling_stdev_output.csv`

## 📦 Setup

Install dependencies:

```bash
pip install -r requirements.txt
```

## 📝 Notes

- Both apps will log progress and performance (time, memory).
- Results are written as `.csv` files into the respective `results/` folders.
- Modify the `__main__` block in each script to change parameters or file paths.