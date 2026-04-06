#!/usr/bin/env python3
"""Simple script to trigger data sync for stock 000001.SZ"""

import sys
import os
from datetime import date
from pathlib import Path

# Add the project root to Python path
sys.path.insert(0, '/home/aaron/codespace/quantIDE')

import polars as pl
from quantide.data.fetchers.tushare import fetch_bars_ext
from quantide.data.models.daily_bars import daily_bars
from quantide.data import init_data

def main():
    print("Starting data sync for 000001.SZ")
    
    # Initialize data layer
    print("Initializing data layer...")
    try:
        # Use the default quantide data directory
        init_data(Path.home() / ".quantide")
        print("✓ Data layer initialized")
    except Exception as e:
        print(f"✗ Failed to initialize data layer: {e}")
        return False
    
    # Define specific dates to fetch (avoiding weekends)
    dates = [
        date(2026, 2, 24),  # Tuesday
        date(2026, 2, 25),  # Wednesday
        date(2026, 2, 26),  # Thursday
        date(2026, 2, 27),  # Friday
        date(2026, 3, 2),   # Monday
        date(2026, 3, 3),   # Tuesday
        date(2026, 3, 4),   # Wednesday
        date(2026, 3, 5),   # Thursday
        date(2026, 3, 6),   # Friday
        date(2026, 3, 9),   # Monday
    ]
    
    print(f"Fetching data for {len(dates)} specific dates...")
    
    # Fetch data for the dates
    try:
        df, errors = fetch_bars_ext(dates)
        
        if errors:
            print(f"Errors encountered during fetch: {errors}")
        
        if df is None or len(df) == 0:
            print("No data returned from fetch_bars_ext")
            return False
            
        print(f"Fetched {len(df)} rows of data")
        print(f"Columns: {list(df.columns)}")
        
        # Check that we have all required fields
        required_fields = ['date', 'asset', 'open', 'high', 'low', 'close', 'volume', 'amount', 'adjust', 'is_st', 'up_limit', 'down_limit']
        missing_fields = [field for field in required_fields if field not in df.columns]
        
        if missing_fields:
            print(f"Missing required fields: {missing_fields}")
            return False
            
        print("All required fields present")
        
        # Check for null values in critical fields
        critical_fields = ['up_limit', 'down_limit', 'is_st', 'adjust']
        for field in critical_fields:
            null_count = df[field].isnull().sum()
            if null_count > 0:
                print(f"Warning: {null_count} null values in {field}")
            else:
                print(f"✓ {field}: no null values")
        
        # Filter for our specific stock
        stock_data = df[df['asset'] == '000001.SZ'].copy()
        if len(stock_data) == 0:
            print("No data found for stock 000001.SZ")
            # Let's see what assets we DO have
            assets = df['asset'].unique()
            print(f"Available assets: {assets[:10]}")  # Show first 10
            return False
            
        print(f"Found {len(stock_data)} rows for 000001.SZ")
        
        # Sort by date
        stock_data = stock_data.sort_values('date')
        
        # Save to local storage via daily_bars.store
        print("Saving data to local storage...")
        try:
            daily_bars.store.append_data(stock_data)
            print("✓ Data saved successfully")
        except Exception as e:
            print(f"✗ Error saving data: {e}")
            import traceback
            traceback.print_exc()
            return False
            
        # Verify the saved data
        print("Verifying saved data...")
        try:
            # Try to retrieve the data we just saved
            verify_start = date(2026, 2, 24)
            verify_end = date(2026, 3, 9)
            
            retrieved_df = daily_bars.get_bars_in_range(
                start=verify_start,
                end=verify_end,
                assets=['000001.SZ'],
                eager_mode=True
            )
            
            if retrieved_df is not None and len(retrieved_df) > 0:
                print(f"✓ Verification successful: retrieved {len(retrieved_df)} rows")
                
                # Check specific fields
                for field in ['up_limit', 'down_limit', 'is_st', 'adjust']:
                    if field in retrieved_df.columns:
                        null_count = retrieved_df.select(pl.col(field).is_null().sum()).item()
                        if null_count == 0:
                            print(f"✓ {field}: all values present")
                        else:
                            print(f"⚠ {field}: {null_count} null values")
                    else:
                        print(f"✗ {field}: field not found")
                        
            else:
                print("✗ Verification failed: no data retrieved")
                return False
                
        except Exception as e:
            print(f"✗ Error during verification: {e}")
            import traceback
            traceback.print_exc()
            return False
            
        print("\n✓ Data sync completed successfully!")
        return True
        
    except Exception as e:
        print(f"✗ Error during data fetch: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)