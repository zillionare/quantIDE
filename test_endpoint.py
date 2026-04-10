"""Test what the endpoint actually returns by simulating the server logic"""

import os
os.environ['QUANTIDE_DATA_HOME'] = str(Path.home() / ".quantide")

# Initialize data like the server does
from pathlib import Path
from quantide.data import init_data

home_path = Path.home() / ".quantide"
print(f"Initializing data from: {home_path}")
init_data(home=home_path, init_db=False)

# Now test the exact same function call as the endpoint
from quantide.web.pages.system.market import _get_market_data

print("\n=== Testing _get_market_data directly ===")
data, total = _get_market_data(
    code="000001.SZ",
    start_date="2026-02-24",
    end_date="2026-04-03",
    adjust="none",
    page=1,
    per_page=20
)

print(f"Total: {total}")
print(f"Data rows: {len(data)}")

if data:
    print("First row:")
    for k, v in data[0].items():
        print(f"  {k}: {v}")
else:
    print("NO DATA RETURNED")
    
    # Let's debug step by step
    print("\n=== Debugging _get_market_data ===")
    from quantide.data.models.daily_bars import daily_bars
    import datetime
    
    print(f"daily_bars._store: {daily_bars._store}")
    
    if daily_bars._store is not None:
        try:
            start_dt = datetime.datetime.strptime("2026-02-24", "%Y-%m-%d").date()
            end_dt = datetime.datetime.strptime("2026-04-03", "%Y-%m-%d").date()
            
            print(f"Calling get_bars_in_range with:")
            print(f"  start: {start_dt}")
            print(f"  end: {end_dt}")
            print(f"  assets: ['000001.SZ']")
            print(f"  adjust: None (since adjust='none')")
            
            df = daily_bars.get_bars_in_range(
                start=start_dt,
                end=end_dt,
                assets=["000001.SZ"],
                adjust=None,  # This is what gets passed when adjust="none"
                eager_mode=True,
            )
            
            print(f"get_bars_in_range returned: {df}")
            if df is not None:
                print(f"DataFrame shape: {df.shape}")
                if hasattr(df, 'to_pandas'):
                    pandas_df = df.to_pandas()
                    print(f"Pandas DataFrame shape: {pandas_df.shape}")
                    print(pandas_df.head())
            
        except Exception as e:
            print(f"Error in get_bars_in_range: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("daily_bars store is None!")