"""Take a full-page screenshot of the market page HTML using Playwright"""

import asyncio
from pathlib import Path
from playwright.async_api import async_playwright


async def main():
    html_path = "/tmp/market_page_rendered.html"
    screenshot_path = "/home/aaron/codespace/quantIDE/market_page_final_v5.png"
    
    print(f"Checking if HTML file exists: {html_path}")
    if not Path(html_path).exists():
        print(f"ERROR: {html_path} does not exist!")
        return
    
    print(f"Launching Playwright browser...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await context.new_page()
        
        # Set up console log capture
        console_logs = []
        page.on("console", lambda msg: console_logs.append(f"[{msg.type}] {msg.text}"))
        page.on("pageerror", lambda err: console_logs.append(f"[PAGE ERROR] {err}"))
        
        try:
            # Navigate to local file
            file_url = f"file://{html_path}"
            print(f"Navigating to: {file_url}")
            await page.goto(file_url, wait_until="networkidle")
            
            # Wait for table body rows to be visible
            print("Waiting for tbody tr to be visible...")
            try:
                await page.wait_for_selector("tbody tr", state="visible", timeout=10000)
                print("✓ Table rows are visible!")
            except Exception as e:
                print(f"✗ Failed to find table rows: {e}")
                print(f"Saving console logs and blank page screenshot...")
                
                # Save console logs
                log_path = "/tmp/market_screenshot_logs.txt"
                with open(log_path, "w") as f:
                    f.write("\n".join(console_logs))
                print(f"Console logs saved to: {log_path}")
                
                # Take screenshot of blank page
                blank_screenshot = "/tmp/market_screenshot_blank.png"
                await page.screenshot(path=blank_screenshot, full_page=True)
                print(f"Blank screenshot saved to: {blank_screenshot}")
                await browser.close()
                return
            
            # Take full-page screenshot
            print(f"Taking full-page screenshot...")
            await page.screenshot(path=screenshot_path, full_page=True)
            print(f"Screenshot saved to: {screenshot_path}")
            
            # Verify the screenshot exists
            if Path(screenshot_path).exists():
                size = Path(screenshot_path).stat().st_size
                print(f"Screenshot size: {size:,} bytes")
            else:
                print(f"ERROR: Screenshot file was not created!")
            
        except Exception as e:
            print(f"ERROR during screenshot: {e}")
            print(f"Saving console logs...")
            
            log_path = "/tmp/market_screenshot_logs.txt"
            with open(log_path, "w") as f:
                f.write("\n".join(console_logs))
            print(f"Console logs saved to: {log_path}")
            
            # Try to take whatever screenshot we can
            try:
                error_screenshot = "/tmp/market_screenshot_error.png"
                await page.screenshot(path=error_screenshot, full_page=True)
                print(f"Error screenshot saved to: {error_screenshot}")
            except:
                pass
        
        finally:
            await browser.close()
    
    print("\n=== Screenshot generation complete ===")


if __name__ == "__main__":
    asyncio.run(main())
