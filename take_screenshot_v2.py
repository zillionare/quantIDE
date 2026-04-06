"""Robust screenshot script with explicit wait for table data"""

import asyncio
import sys
import os
from pathlib import Path

# Initialize data layer before importing anything that might use it
def init_data_layer():
    """Ensure data is properly initialized"""
    os.environ['QUANTIDE_DATA_HOME'] = str(Path.home() / ".quantide")
    
    from quantide.data import init_data
    from quantide.data.models.daily_bars import daily_bars
    
    home_path = Path.home() / ".quantide"
    print(f"Initializing data layer from: {home_path}")
    init_data(home=home_path, init_db=False)
    
    # Verify it's working
    if daily_bars._store is None:
        raise RuntimeError("Failed to initialize daily_bars store")
    print("Data layer initialized successfully")

# Initialize before importing playwright or doing web stuff
init_data_layer()

import asyncio
from playwright.async_api import async_playwright

URL = "http://localhost:5001/system/market?code=000001.SZ&start_date=2026-02-24&end_date=2026-04-03"
OUTPUT_PATH = "/home/aaron/codespace/quantIDE/market_page_final_v4.png"
TIMEOUT_MS = 15000  # 15 seconds

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await context.new_page()
        
        # Collect console logs for error reporting
        console_logs = []
        page.on("console", lambda msg: console_logs.append(f"[{msg.type}] {msg.text}"))
        
        try:
            print(f"Navigating to: {URL}")
            await page.goto(URL, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
            
            # Crucial: Wait for data rows to actually render
            print("Waiting for table data rows (tbody tr)...")
            await page.wait_for_selector('tbody tr', timeout=TIMEOUT_MS)
            
            # Additional verification - ensure it's not the "no data" row
            rows = await page.query_selector_all('tbody tr')
            if rows:
                # Check if there's actual content (not just "暂无数据")
                first_row_text = await rows[0].text_content()
                if "暂无数据" in first_row_text:
                    print(f"WARNING: Table exists but shows no data message: {first_row_text.strip()}")
                else:
                    print(f"SUCCESS: Found {len(rows)} data rows")
            
            # Full page screenshot
            await page.screenshot(path=OUTPUT_PATH, full_page=True)
            print(f"Screenshot saved to: {OUTPUT_PATH}")
            
        except Exception as e:
            print(f"ERROR: Failed to capture screenshot - {e}")
            
            # Save error state
            error_screenshot = "/home/aaron/codespace/quantIDE/market_page_error.png"
            try:
                await page.screenshot(path=error_screenshot, full_page=True)
                print(f"Error state screenshot saved to: {error_screenshot}")
            except:
                pass
            
            # Output console logs for debugging
            print("\n=== Browser Console Logs ===")
            for log in console_logs:
                print(log)
            print("===========================\n")
            
            sys.exit(1)
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(main())