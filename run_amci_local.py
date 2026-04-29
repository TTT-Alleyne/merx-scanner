"""
run_amci_local.py — Alleyne Group
Standalone AMCI scanner for Windows. Run this directly, NOT via Docker.

Why not Docker? Cloudflare Turnstile requires manual login in a visible browser.
Docker containers on Windows can't show browser windows easily.
This script runs on your local machine and uploads results to Nextcloud directly.

HOW TO RUN:
  1. Open terminal in merx-scanner folder
  2. pip install playwright python-dotenv requests openpyxl
  3. playwright install chromium
  4. python run_amci_local.py

Cookie is cached in results/amci_cookies.json — valid ~25 days.
After that, just run again and log in fresh.
"""

import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load .env from same folder as this script
load_dotenv(Path(__file__).parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# Add the merx-scanner folder to path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent))


async def main():
    log.info("=" * 60)
    log.info("AMCI Scanner — Alleyne Group (local mode)")
    log.info("=" * 60)

    # Load criteria
    criteria_file = os.getenv("CRITERIA_FILE", str(Path(__file__).parent / "data" / "criteria_sets.json"))
    try:
        with open(criteria_file) as f:
            data = json.load(f)
        caps, sigs, profiles = [], [], []
        for s in data["sets"]:
            if s["company"] != os.getenv("COMPANY", "Alleyne Inc."):
                continue
            active = [i for i in s["items"] if i.get("active", True)]
            if s["type"] == "capabilities":
                caps = active
            elif s["type"] == "sales":
                sigs = active
            elif s["type"] == "search":
                profiles = active
        log.info(f"Criteria loaded: {len(caps)} capabilities, {len(sigs)} signals, {len(profiles)} profiles")
    except Exception as e:
        log.error(f"Could not load criteria: {e}")
        log.error(f"Expected at: {criteria_file}")
        return

    # Load known clients
    known_clients = []
    try:
        kc_file = os.getenv("KNOWN_CLIENTS", str(Path(__file__).parent / "known_clients.json"))
        with open(kc_file) as f:
            known_clients = json.load(f).get("known_clients", [])
        log.info(f"Loaded {len(known_clients)} known clients")
    except Exception as e:
        log.warning(f"Could not load known clients: {e}")

    # Run AMCI scan
    from amci_driver import run_amci_scan
    opportunities = await run_amci_scan(
        search_profiles=profiles,
        capabilities=caps,
        signals=sigs,
        known_clients=known_clients,
        min_days=0,
        max_results=200,
    )

    if not opportunities:
        log.warning("No AMCI opportunities found — nothing to upload")
        return

    log.info(f"Found {len(opportunities)} AMCI opportunities")

    # Upload to Nextcloud via nextcloud_writer
    try:
        from nextcloud_writer import write_to_nextcloud
        company = os.getenv("COMPANY", "Alleyne Inc.")
        url = write_to_nextcloud(
            opportunities=opportunities,
            known_clients=known_clients,
            company=company,
            profile_name="All-Profiles",
            platform="AMCI"
        )
        log.info(f"✅ Uploaded to Nextcloud: {url}")
    except Exception as e:
        log.error(f"Nextcloud upload failed: {e}")
        # Save locally as fallback
        out = Path(__file__).parent / "results" / "amci_results.json"
        out.parent.mkdir(exist_ok=True)
        out.write_text(json.dumps(opportunities, indent=2, default=str))
        log.info(f"Saved locally to {out}")

    log.info("=" * 60)
    log.info("AMCI scan complete!")
    log.info("=" * 60)

    # Print top opportunities
    top = sorted(opportunities, key=lambda x: x.get("score", 0), reverse=True)[:5]
    print("\nTOP OPPORTUNITIES:")
    for o in top:
        print(f"  [{o.get('score', 0):3d}] {o.get('title', '')[:60]}")
        print(f"        {o.get('organization', '')} — closes {o.get('closing_date', 'unknown')}")


if __name__ == "__main__":
    asyncio.run(main())
