"""Debug script to check what's happening with market data in server context"""

import os
import sys
from pathlib import Path

# Check env var
print("QUANTIDE_DATA_HOME:", os.environ.get('QUANTIDE_DATA_HOME', 'NOT SET'))

# Import and check the actual singleton state
from quantide.data.models.daily_bars import daily_bars
from quantide.data.models.calendar import calendar
from quantide.data.models.stocks import stock_list

print("\n=== Singleton State ===")
print(f"daily_bars._store: {daily_bars._store}")
print(f"calendar._data is None: {calendar._data is None}")
print(f"stock_list._data is None: {stock_list._data is None}")

if daily_bars._store is not None:
    print(f"daily_bars store path: {getattr(daily_bars._store, 'root', 'NO ROOT ATTR')}")
else:
    print("daily_bars store is NOT initialized")

# Try to get data
print("\n=== Testing _get_market_data ===")
from quantide.web.pages.system.market import _get_market_data

data, total = _get_market_data(
    code="000001.SZ",
    start_date="2026-02-24",
    end_date="2026-04-03",
    adjust="none",
    page=1,
    per_page=20
)

print(f"Total records: {total}")
print(f"Data length: {len(data)}")

if data:
    print("First row:", data[0])
else:
    print("No data returned")

# Now let's try to fix it by reconnecting with the right path
print("\n=== Attempting to fix by reconnecting ===")
from quantide.config.settings import get_data_home

data_home = get_data_home()
print(f"Data home from settings: {data_home}")

daily_bars_path = Path(data_home) / "data" / "bars" / "daily"
calendar_path = Path(data_home) / "data" / "calendar.parquet"

print(f"Connecting daily_bars to: {daily_bars_path}")
print(f"Calendar path: {calendar_path}")

try:
    daily_bars.connect(str(daily_bars_path), str(calendar_path))
    print("Reconnected successfully!")
    
    # Try again
    data, total = _get_market_data(
        code="000001.SZ",
        start_date="2026-02-24",
        end_date="2026-04-03",
        adjust="none",
        page=1,
        per_page=20
    )
    
    print(f"After fix - Total records: {total}")
    print(f"After fix - Data length: {len(data)}")
    if data:
        print("First row after fix:", data[0])
        
except Exception as e:
    print(f"Failed to reconnect: {e}")
    import traceback
    traceback.print_exc()