import asyncio
import aiohttp
import csv
import os
import random
import sys
import time

# --- CONFIGURATION ---
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
GOLD_CSV = os.path.join(SCRIPT_DIR, "subreddits_gold_promo_safe.csv")
PROGRESS_FILE = os.path.join(SCRIPT_DIR, "master_scraper_progress.csv")

CONCURRENCY = 3
BACKOFF_TIME = 90  # Seconds to wait after 429
POPULAR_PAGES = 150  # 150 pages * 100 = ~15,000 top subreddits

RISK_KEYWORDS = ["promo", "advertising", "spam", "self-promo", "advertise", "marketing", "no links", "only news", "affiliate", "shilling"]

async def fetch_popular_subreddits(session):
    print(f"\n[PHASE 1] Fetching up to {POPULAR_PAGES * 100} top subreddits...")
    allowed_subs = []
    after = None
    
    for page in range(POPULAR_PAGES):
        url = "https://old.reddit.com/subreddits/popular.json?limit=100"
        if after: url += f"&after={after}"
        
        retries = 3
        while retries > 0:
            try:
                async with session.get(url, headers=HEADERS, timeout=15) as r:
                    if r.status == 200:
                        data = await r.json(content_type=None)
                        children = data.get("data", {}).get("children", [])
                        after = data.get("data", {}).get("after")
                        
                        for child in children:
                            d = child.get("data", {})
                            sub_type = d.get("submission_type", "any")
                            if sub_type in ("any", "link"):
                                allowed_subs.append({
                                    "name": f"r/{d.get('display_name')}",
                                    "subscribers": d.get("subscribers", 0)
                                })
                        break # Success
                    elif r.status == 429:
                        wait = 45 + random.random() * 20
                        print(f"  [!] Rate limited on Phase 1 Page {page+1}. Pausing {wait:.0f}s...")
                        await asyncio.sleep(wait)
                        retries -= 1
                    else:
                        break # Other error
            except Exception as e:
                await asyncio.sleep(5)
                retries -= 1
                
        if (page + 1) % 10 == 0:
            print(f"  -> Page {page+1}/{POPULAR_PAGES}: Found {len(allowed_subs)} link-friendly subreddits so far")
            
        if not after: break
        await asyncio.sleep(2 + random.random() * 2)
        
    # Deduplicate
    unique = {sub["name"]: sub for sub in allowed_subs}
    return list(unique.values())

async def fetch_rules(session, sub_name):
    url = f"https://old.reddit.com/{sub_name}/about/rules.json"
    while True:
        try:
            async with session.get(url, headers=HEADERS, timeout=15) as r:
                if r.status == 200:
                    data = await r.json(content_type=None)
                    rules = data.get("rules", [])
                    all_text = " ".join([ru.get("short_name", "") + " " + ru.get("description", "") for ru in rules]).lower()
                    return all_text, "Success"
                elif r.status == 429:
                    return None, "RateLimit"
                elif r.status == 404:
                    return None, "NotFound"
                else:
                    return None, "Error"
        except Exception:
            return None, "Exception"

async def process_sub(session, row, sem, stats, f, writer):
    async with sem:
        rules_text, status = await fetch_rules(session, row['name'])
        
        if status == "RateLimit":
            print(f"  [!] Rate Limit in Phase 2 for {row['name']}. Master pause for {BACKOFF_TIME}s...")
            stats['rate_limits'] += 1
            await asyncio.sleep(BACKOFF_TIME)
            rules_text, status = await fetch_rules(session, row['name']) # Retry once
            
        if status == "Success" and rules_text is not None:
            # STRICT FILTERING
            if any(w in rules_text for w in RISK_KEYWORDS):
                stats['rejected'] += 1
            else:
                # IT IS GOLD!
                notes = []
                if "karma" in rules_text: notes.append("Requires Karma")
                if "account age" in rules_text or "days old" in rules_text: notes.append("Account Age Limit")
                
                row["notes"] = " | ".join(notes)
                writer.writerow(row)
                f.flush()
                stats['gold'] += 1
        else:
             stats['rejected'] += 1
             
        stats['checked'] += 1
        if stats['checked'] % 50 == 0:
            print(f"  Progress: {stats['checked']}/{stats['total']} | Gold Kept: {stats['gold']} | Rejected: {stats['rejected']}")
            
        await asyncio.sleep(1.5 + random.random() * 2)

async def main():
    print("="*60)
    print("MASTER SCRAPER: BUILDING 'GOLD' LIST")
    print("="*60)
    
    async with aiohttp.ClientSession() as session:
        # Phase 1: Get base list
        if os.path.exists(PROGRESS_FILE):
             print(f"Found existing progress in {PROGRESS_FILE}, loading...")
             with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                 subs_to_check = list(csv.DictReader(f))
        else:
             subs_to_check = await fetch_popular_subreddits(session)
             with open(PROGRESS_FILE, "w", newline="", encoding="utf-8") as f:
                 w = csv.DictWriter(f, fieldnames=["name", "subscribers"])
                 w.writeheader()
                 w.writerows(subs_to_check)
        
        # Sort by size to check largest first
        subs_to_check.sort(key=lambda x: int(x.get("subscribers", 0)), reverse=True)
        
        print(f"\n[PHASE 2] Checking rules for {len(subs_to_check)} link-friendly subreddits...")
        print(f"-> Rejecting any subreddits with anti-promo rules.")
        print(f"-> Saving 'Gold' results directly to: {GOLD_CSV}")
        
        # Determine already processed size to resume
        done_names = set()
        if os.path.exists(GOLD_CSV):
            with open(GOLD_CSV, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    done_names.add(row["name"])
                    
        pending = [s for s in subs_to_check if s["name"] not in done_names]
        print(f"-> {len(done_names)} already processed. {len(pending)} remaining to check.")
        
        stats = {'checked': 0, 'gold': len(done_names), 'rejected': 0, 'rate_limits': 0, 'total': len(pending)}
        sem = asyncio.Semaphore(CONCURRENCY)
        
        write_header = not os.path.exists(GOLD_CSV) or os.path.getsize(GOLD_CSV) == 0
        with open(GOLD_CSV, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["name", "subscribers", "notes"])
            if write_header: writer.writeheader()
            
            # Process in small chunk blocks
            chunk_size = 50
            for i in range(0, len(pending), chunk_size):
                chunk = pending[i:i+chunk_size]
                tasks = [process_sub(session, row, sem, stats, f, writer) for row in chunk]
                await asyncio.gather(*tasks)

    print("\n" + "="*60)
    print(f"DONE! Master Scrape Complete.")
    print(f"Saved {stats['gold']} high-quality, promo-safe subreddits to:")
    print(GOLD_CSV)
    print("="*60)

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
