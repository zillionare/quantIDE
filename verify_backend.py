"""Verify backend data for 000001.SZ in the range 2026-02-24 to 2026-04-03"""

import sys
import datetime
from pathlib import Path

# Add the project to the path
sys.path.insert(0, str(Path(__file__).parent))

# Initialize the data layer (same as web app)
from quantide.config.paths import normalize_data_home
from quantide.data import init_data
from quantide.data.sqlite import db
from quantide.config.paths import get_app_db_path

# Initialize database and data
db.init(get_app_db_path())
home = normalize_data_home()
init_data(home, init_db=False)

# Import and test the _get_market_data function
from quantide.web.pages.system.market import _get_market_data

# Test with the actual available date range (since we only have data until 2026-03-09)
code = "000001.SZ"
start_date = "2026-02-24"
end_date = "2026-03-09"  # Using actual end date instead of 2026-04-03

print(f"Fetching data for {code} from {start_date} to {end_date}...")
data, total = _get_market_data(code=code, start_date=start_date, end_date=end_date, adjust="none")

print(f"Total records: {total}")
print(f"Number of rows returned (first page): {len(data)}")

if data:
    print("\nFirst row:")
    first_row = data[0]
    for key, value in sorted(first_row.items()):
        print(f"  {key}: {value} (type: {type(value).__name__})")
    
    # Check critical columns
    required_columns = ["up_limit", "down_limit", "open", "high", "low", "close", "volume", "amount"]
    print("\nColumn checklist:")
    for col in required_columns:
        present = col in first_row
        status = "✓" if present else "✗ MISSING"
        print(f"  {col}: {status}")
        
    # Also show the actual values for limit columns
    print(f"\nLimit values:")
    print(f"  up_limit: {first_row.get('up_limit', 'MISSING')}")
    print(f"  down_limit: {first_row.get('down_limit', 'MISSING')}")
else:
    print("ERROR: No data returned!")

# Also test with the originally requested range to show what happens
print(f"\n{'='*60}")
print("Testing with originally requested range (2026-02-24 to 2026-04-03):")
print("(This will show empty data because our stored data only goes to 2026-03-09)")
data2, total2 = _get_market_data(code=code, start_date="2026-02-24", end_date="2026-04-03", adjust="none")
print(f"Total records: {total2}")
print(f"Number of rows returned: {len(data2)}")