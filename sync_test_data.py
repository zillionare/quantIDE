#!/usr/bin/env python3
"""Sync and verify market data for 000001.SZ"""
import datetime
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from quantide.data import init_data
from quantide.data.models.daily_bars import daily_bars
from quantide.data.models.calendar import calendar

def main():
    # Initialize data layer
    home = project_root / "data"
    init_data(home, init_db=False)
    
    # Define date range
    start_date = datetime.date(2026, 2, 24)
    end_date = datetime.date(2026, 4, 3)
    asset = "000001.SZ"
    
    print(f"Syncing data for {asset} from {start_date} to {end_date}...")
    
    # Sync data for the date range
    try:
        daily_bars.store.fetch_with_daily_progress(
            start=start_date,
            end=end_date,
            force=False  # Only fetch missing dates
        )
        print(f"Sync completed.")
    except Exception as e:
        print(f"Sync error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    # Verify data is present
    print(f"\nQuerying data for {asset}...")
    df = daily_bars.get_bars_in_range(
        start=start_date,
        end=end_date,
        assets=[asset],
        adjust=None,  # No adjustment to see raw fields
        eager_mode=True,
    )
    
    print(f"Retrieved {len(df)} rows")
    
    if len(df) > 0:
        print("\nDataFrame columns:", df.columns)
        print("\nFirst row sample:")
        print(df.head(1))
        
        # Check required fields
        required_fields = ['date', 'asset', 'open', 'high', 'low', 'close', 
                          'volume', 'amount', 'adjust', 'is_st', 'up_limit', 'down_limit']
        missing = [f for f in required_fields if f not in df.columns]
        if missing:
            print(f"\n❌ MISSING FIELDS: {missing}")
        else:
            print(f"\n✅ All 12 required fields are present!")
            
        # Show specific field values from first row
        first = df.row(0, named=True)
        print(f"\nSample values from first row:")
        print(f"  date: {first.get('date')}")
        print(f"  asset: {first.get('asset')}")
        print(f"  open: {first.get('open')}")
        print(f"  close: {first.get('close')}")
        print(f"  adjust: {first.get('adjust')}")
        print(f"  is_st: {first.get('is_st')}")
        print(f"  up_limit: {first.get('up_limit')}")
        print(f"  down_limit: {first.get('down_limit')}")
    else:
        print("❌ No data retrieved!")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
