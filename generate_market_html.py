"""Generate market page HTML and save to /tmp/market_page_rendered.html"""

import asyncio
import sys
from pathlib import Path

# Add the project to the path
sys.path.insert(0, str(Path(__file__).parent))

# Initialize data layer first
from pathlib import Path as PathLib
project_root = PathLib(__file__).parent
from quantide.data import init_data

init_data(project_root / "data", init_db=False)

from quantide.web.pages.system.market import market_page


class FakeRequest:
    """Simulate FastHTML request with query_params"""
    def __init__(self, query_params: dict):
        self.query_params = query_params


async def main():
    # Simulate request parameters
    code = "000001.SZ"
    start_date = "2026-02-24"
    end_date = "2026-04-03"
    adjust = "none"
    page = 1
    per_page = 20

    # Create a fake request object
    fake_req = FakeRequest(query_params={
        "code": code,
        "start_date": start_date,
        "end_date": end_date,
        "adjust": adjust,
        "page": str(page),
        "per_page": str(per_page),
    })

    print(f"Calling market_page with code={code}, start_date={start_date}, end_date={end_date}, adjust={adjust}, page={page}, per_page={per_page}")

    # Call the market_page function
    html_response = await market_page(fake_req, code=code, start_date=start_date, end_date=end_date, adjust=adjust, page=page, per_page=per_page)

    # Extract HTML content
    html_content = html_response.body.decode("utf-8") if isinstance(html_response.body, bytes) else html_response.body

    # Save to file
    output_path = "/tmp/market_page_rendered.html"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"\nHTML saved to {output_path}")
    print(f"HTML size: {len(html_content)} bytes")

    # Extract and print tbody snippet for verification
    tbody_start = html_content.find("<tbody")
    tbody_end = html_content.find("</tbody>") + len("</tbody>")
    
    if tbody_start != -1 and tbody_end != -1:
        tbody_content = html_content[tbody_start:tbody_end]
        print("\n=== TBODY SNIPPET (first 1000 chars) ===")
        print(tbody_content[:1000])
        
        # Count rows
        row_count = tbody_content.count("<tr")
        print(f"\n=== ROW COUNT: {row_count} ===")
        
        if row_count > 0:
            print("✓ Data rows present in tbody")
        else:
            print("✗ No data rows found in tbody")
    else:
        print("✗ Could not find tbody in HTML")


if __name__ == "__main__":
    asyncio.run(main())
