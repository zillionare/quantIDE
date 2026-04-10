#!/usr/bin/env python3
"""Script to trigger data sync for stock 000001.SZ from 2026-02-24 to 2026-04-03"""

import sys
import os
from datetime import datetime, date

# Add the project root to Python path
sys.path.insert(0, '/home/aaron/codespace/quantIDE')

from quantide.data.fetchers.tushare import fetch_bars_ext
from quantide.data.models.daily_bars import daily_bars

def main():
    print("Starting data sync for 000001.SZ from 2026-02-24 to 2026-04-03")
    
    # Define date range
    start_date = date(2026, 2, 24)
    end_date = date(2026, 4, 3)
    
    # Generate list of dates (we'll fetch for each trading day)
    # For simplicity, let's fetch for a sample of dates
    dates = []
    current = start_date
    while current <= end_date:
        # Skip weekends for simplicity (this is approximate)
        if current.weekday() < 5:  # Monday=0, Friday=4
            dates.append(current)
        # Move to next day
        from datetime import timedelta
        current = current + timedelta(days=1)
    
    print(f"Generated {len(dates)} dates to fetch")
    
    # Fetch data for the date range
    print("Fetching data using fetch_bars_ext...")
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
            return False
            
        print(f"Found {len(stock_data)} rows for 000001.SZ")
        
        # Sort by date
        stock_data = stock_data.sort_values('date')
        
        # Save to local storage via daily_bars.store
        print("Saving data to local storage...")
        try:
            daily_bars.store(stock_data)
            print("✓ Data saved successfully")
        except Exception as e:
            print(f"✗ Error saving data: {e}")
            return False
            
        # Verify the saved data
        print("Verifying saved data...")
        try:
            # Try to retrieve the data we just saved
            verify_start = date(2026, 2, 24)
            verify_end = date(2026, 4, 3)
            
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
                        null_count = retrieved_df[field].isnull().sum()
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