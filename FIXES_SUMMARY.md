## Summary of Fixes Applied

### 1. Data Fields Fix (quantide/data/fetchers/tushare.py)
- Modified `fetch_bars()` function to request all required fields:
  - up_limit, down_limit (from fetch_limit_price)
  - is_st (from fetch_st_info)
  - adjust (from fetch_adjust_factor)
- Added proper merging of data from all sources
- Added fallback values for missing data (0.0 for prices, False for is_st, 1.0 for adjust)

### 2. UI/Style Fixes (quantide/web/pages/system/market.py)
- Fixed field name mismatch: changed from 'trade_date' to 'date' to match backend
- Fixed missing 'pre_close' field: added fallback to 'open' price
- Verified colspan='12' is correct for 12-column table

### 3. Cache Reset
- Cleared existing cache at ~/.config/quantide/data/bars/daily/
- This will trigger a fresh fetch with the complete schema on next request

### 4. Screenshot Location
- Screenshot saved to: /home/aaron/codespace/quantIDE/market_page_fixed_v2.png