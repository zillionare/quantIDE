from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

def take_market_screenshot():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    
    # 指定 browser 和 driver 路径
    options.binary_location = "/snap/chromium/current/usr/lib/chromium-browser/chrome"
    service = Service(executable_path="/usr/bin/chromedriver")
    
    driver = webdriver.Chrome(service=service, options=options)
    
    try:
        # Navigate to the market page with the specified parameters
        url = "http://localhost:5001/system/market?code=000001.SZ&start_date=2026-02-24&end_date=2026-04-03"
        driver.get(url)
        
        # Wait for the table to load - look for the table element
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//table[contains(@class, 'min-w-full')]"))
        )
        
        # 额外等待 2 秒让数据完全渲染
        time.sleep(3)
        
        # Save screenshot
        driver.save_screenshot("/home/aaron/codespace/quantIDE/market_page_final_check.png")
        print("Market page screenshot saved successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        driver.quit()

if __name__ == "__main__":
    take_market_screenshot()