from firecrawl import FirecrawlApp
import pandas as pd
import re

FIRECRAWL_API_KEY = ""
app = FirecrawlApp(api_key=FIRECRAWL_API_KEY)


result = app.extract(
    ["https://www.canstar.com.au/health-insurance/"],
    prompt="""
        Extract all featured and sponsored health insurance offers on this page.
        For each offer return:
        - provider: insurer/brand name
        - product_name: plan name
        - cover_type: e.g. Hospital, Extras, Combined
        - promotion: any special offer or discount text
        - url: link to the offer page
    """,
    schema={
        "type": "object",
        "properties": {
            "offers": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "provider":     {"type": "string"},
                        "product_name": {"type": "string"},
                        "cover_type":   {"type": "string"},
                        "promotion":    {"type": "string"},
                        "url":          {"type": "string"}
                    }
                }
            }
        }
    }
)

offers = result.data.get("offers", []) if hasattr(result, "data") else []

if not offers:
    print("Falling back to plain scrape...")
    scrape_result = app.scrape(
        "https://www.canstar.com.au/health-insurance/",
        formats=["markdown"],
        wait_for=3000
    )
    markdown = scrape_result.markdown or ""

    for block in markdown.split("\n\n"):
        if any(kw in block.lower() for kw in ["hospital", "extras", "cover", "/month", "/week"]):
            price_match = re.search(r"\$[\d,]+(?:\.\d{2})?(?:/(?:month|week|mth|wk))?", block, re.I)
            offers.append({
                "raw_text": block.strip()[:300],
                "price": price_match.group(0) if price_match else ""
            })

# --- Save to CSV ---
if offers:
    df = pd.DataFrame(offers)
    df.to_csv("canstar_health_insurance_offers.csv", index=False, encoding="utf-8-sig")
    print(f"Saved {len(offers)} offers to canstar_health_insurance_offers.csv")
    print(df.head().to_string(max_colwidth=60))
else:
    print("No offers found.")