"""Backend verification script for _get_market_data"""

import sys
from pathlib import Path

# Initialize data layer before importing market
home_path = Path.home() / ".quantide"
from quantide.data import init_data

print(f"Initializing data layer from: {home_path}")
init_data(home=home_path, init_db=False)

from quantide.web.pages.system.market import _get_market_data

code = "000001.SZ"
start_date = "2026-02-24"
end_date = "2026-04-03"
adjust = "none"

print(f"Testing _get_market_data with:")
print(f"  code={code}")
print(f"  start_date={start_date}")
print(f"  end_date={end_date}")
print(f"  adjust={adjust}")
print()

data, total = _get_market_data(code=code, start_date=start_date, end_date=end_date, adjust=adjust, page=1, per_page=20)

if not data:
    print("ERROR: No data returned!")
    sys.exit(1)

print(f"Total records: {total}")
print(f"Records in current page: {len(data)}")
print()

first_row = data[0]
print("First row fields:")
for key, value in first_row.items():
    print(f"  {key}: {value}")

# Check required fields
required_fields = ["up_limit", "down_limit", "is_st", "adjust"]
print()
print("Checking required fields:")
all_present = True
for field in required_fields:
    if field in first_row:
        print(f"  ✓ {field}: present ({first_row[field]})")
    else:
        print(f"  ✗ {field}: MISSING")
        all_present = False

if all_present:
    print()
    print("SUCCESS: All required fields are present!")
else:
    print()
    print("FAIL: Some required fields are missing!")
    sys.exit(1)
