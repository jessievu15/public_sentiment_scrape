import asyncio
from datetime import datetime, timedelta, timezone
from playwright.async_api import async_playwright
import json
import os
import argparse
import boto3

# ---- Config ------------------------------------------------------
BRIGHT_DATA_USERNAME = os.getenv("BRIGHT_DATA_USERNAME", "brd-customer-hl_3e349a49-zone-scraping_browser1")
BRIGHT_DATA_API_KEY  = os.getenv("BRIGHT_DATA_API_KEY", "v7pw4vxobm7b")
BRIGHT_DATA_BROWSER_URL = f"wss://{BRIGHT_DATA_USERNAME}:{BRIGHT_DATA_API_KEY}@brd.superproxy.io:9222"
COMPANY_URL = "https://www.linkedin.com/company/medibank/posts/?feedView=all"

# LinkedIn credentials (needed to bypass login wall)
LINKEDIN_EMAIL    = os.getenv("LINKEDIN_EMAIL", "YOUR_LINKEDIN_EMAIL")
LINKEDIN_PASSWORD = os.getenv("LINKEDIN_PASSWORD", "YOUR_LINKEDIN_PASSWORD")

SOURCE   = "linkedin"
DATASET  = "posts"
TIER     = "public-sentiment"
BUCKET   = "p000268ds-medibank-intelligence"

DAYS_BACK   = 7
CUTOFF_DATE = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)

posts_collected = []

async def main(args):
    async with async_playwright() as pw:
        browser = await pw.chromium.connect_over_cdp(BRIGHT_DATA_BROWSER_URL)
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = await context.new_page()
 
        print("Loading Medibank posts page...")
        await page.goto(COMPANY_URL, wait_until="networkidle", timeout=90000)
        print(f"Scraping posts from last {DAYS_BACK} days. Stopping when an older post is found...")
 
        while True:
            await page.evaluate("window.scrollBy(0, 1200)")
            await asyncio.sleep(3)
 
            extracted = await page.evaluate("""
                () => {
                    const items = [];
                    document.querySelectorAll('div.feed-shared-update-v2, article').forEach(el => {
                        const link = el.querySelector('a[href*="activity-"]');
                        const dateEl = el.querySelector('span.update-components-actor__sub-description time, time');
                        const textEl = el.querySelector('.feed-shared-update-v2__description') ||
                                       el.querySelector('.update-components-text');
                        if (link) {
                            const url = link.href;
                            const dateStr = dateEl ? dateEl.getAttribute('datetime') || dateEl.innerText : '';
                            const text = textEl ? textEl.innerText.trim().slice(0, 400) : '';
                            items.push({url, dateStr, text});
                        }
                    });
                    return items;
                }
            """)
 
            added_any = False
            for item in extracted:
                if not item['url'] or any(p['url'] == item['url'] for p in posts_collected):
                    continue
 
                try:
                    if 'T' in item['dateStr'] and item['dateStr'].endswith('Z'):
                        post_date = datetime.fromisoformat(item['dateStr'].replace('Z', '+00:00'))
                    else:
                        post_date = parse_linkedin_date(item['dateStr'])
                except Exception:
                    post_date = datetime.now(timezone.utc) - timedelta(days=30)
 
                if post_date < CUTOFF_DATE:
                    print(f"Reached post older than {DAYS_BACK} days ({post_date.date()}). Stopping scraping.")
                    await browser.close()
                    handle_output(args)
                    return
 
                posts_collected.append({
                    "url": item['url'],
                    "date_posted_raw": item['dateStr'],
                    "post_date": post_date.isoformat(),
                    "text_preview": item['text'],
                })
                added_any = True
                print(f"Added post from {post_date.date()} | {item['url'][-30:]}")
 
            if not added_any and len(posts_collected) > 5:
                print("No more new posts loading. Finished.")
                break
 
            if len(posts_collected) > 200:
                print("Reached safety limit.")
                break
 
        await browser.close()
 
    handle_output(args)
 
def parse_linkedin_date(date_str: str) -> datetime:
    """Fallback parser for '2d ago', '1w ago', etc."""
    date_str = date_str.lower().strip()
    now = datetime.now(timezone.utc)
    if 'ago' in date_str:
        num = int(''.join(filter(str.isdigit, date_str)) or 1)
        if any(x in date_str for x in ['h', 'hr', 'hour']):
            return now - timedelta(hours=num)
        elif 'd' in date_str:
            return now - timedelta(days=num)
        elif 'w' in date_str:
            return now - timedelta(weeks=num)
        elif 'm' in date_str:
            return now - timedelta(days=num * 30)
    return now - timedelta(days=1)
 
 
# ---- Output Helpers ------------------------------------------------------
def build_payload(content: list[dict]) -> dict:
    return {
        "source": SOURCE,
        "tier": TIER,
        "dataset": DATASET,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "url": COMPANY_URL,
        "content": content,
    }
 
 
def save_local(payload: dict, directory: str = ".") -> None:
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = f"{directory}/{SOURCE}_{DATASET}_{date}.json"
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    print(f"\nSaved locally: {path}")
 
 
def upload_to_s3(payload: dict) -> None:
    s3 = boto3.client("s3", region_name="ap-southeast-2")
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"raw/{payload['tier']}/{payload['source']}_{payload['dataset']}_{date}.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False),
        ContentType="application/json",
    )
    print(f"Uploaded to S3: s3://{BUCKET}/{key}")
 
 
def handle_output(args) -> None:
    if not posts_collected:
        print("No posts collected.")
        return
 
    print(f"\nCollected {len(posts_collected)} posts from the last {DAYS_BACK} days.")
    payload = build_payload(posts_collected)
 
    if args.local is not None:
        save_local(payload, args.local)
    else:
        upload_to_s3(payload)

# ---- Main Execution ------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LinkedIn Medibank Posts Scraper")
    parser.add_argument("--local", metavar="DIR", nargs="?", const=".",
                        help="Save JSON locally to DIR instead of uploading to S3 (defaults to current directory)"
    )
    args = parser.parse_args()
 
    asyncio.run(main(args))
