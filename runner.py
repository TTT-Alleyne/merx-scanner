"""
Runner — Alleyne Group Bid Scanner
Runs all enabled platform scanners, combines results, updates master BidTracker.
Supports: Merx (Selenium), Biddingo (Playwright)
Future: AMCI, Bonfire, UNGM — add drivers here.
"""

import os
import json
import asyncio
import logging
from datetime import datetime
from dataclasses import asdict

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
CRITERIA_FILE  = os.getenv("CRITERIA_FILE",  "/data/criteria_sets.json")
KNOWN_CLIENTS  = os.getenv("KNOWN_CLIENTS",  "/app/known_clients.json")
COMPANY        = os.getenv("COMPANY",        "Alleyne Inc.")
MIN_DAYS       = int(os.getenv("MIN_DAYS_TO_BID", "21"))
MAX_RESULTS    = int(os.getenv("MAX_RESULTS", "50"))

# Which platforms to run — set in .env
# PLATFORMS=Merx,Biddingo   or   PLATFORMS=Biddingo   etc.
PLATFORMS = [p.strip() for p in os.getenv("PLATFORMS", "Merx,Biddingo").split(",") if p.strip()]


# ── Criteria loader ───────────────────────────────────────────────────────────
def load_criteria(company: str):
    with open(CRITERIA_FILE) as f:
        data = json.load(f)

    capabilities, signals, search_profiles = [], [], []
    for s in data["sets"]:
        if s["company"] != company:
            continue
        active = [i for i in s["items"] if i.get("active", True)]
        if s["type"] == "capabilities":
            capabilities = active
        elif s["type"] == "sales":
            signals = active
        elif s["type"] == "search":
            search_profiles = active

    log.info(f"Criteria loaded: {len(capabilities)} capabilities, "
             f"{len(signals)} signals, {len(search_profiles)} profiles")
    return capabilities, signals, search_profiles


def load_known_clients() -> list:
    try:
        with open(KNOWN_CLIENTS) as f:
            return json.load(f).get("known_clients", [])
    except Exception as e:
        log.warning(f"Could not load known clients: {e}")
        return []


# ── Platform runners ──────────────────────────────────────────────────────────
def run_merx(capabilities, signals, search_profiles, known_clients) -> list:
    """Run Merx scanner (Selenium). Returns list of opportunity dicts."""
    try:
        from scanner import (
            make_driver, login, search_opportunities,
            extract_opportunity, score_opportunity
        )
        from dataclasses import asdict as dc_asdict
        import time

        log.info("=== Running Merx scanner ===")
        driver = make_driver()
        opportunities = []

        try:
            login(driver)
            urls = search_opportunities(driver, search_profiles)
            log.info(f"Merx: {len(urls)} URLs to process")

            for i, url in enumerate(urls, 1):
                log.info(f"Merx {i}/{len(urls)}: {url}")
                opp = extract_opportunity(driver, url)
                if opp is None:
                    continue
                if 0 < opp.days_to_close < MIN_DAYS:
                    continue
                opp = score_opportunity(opp, capabilities, signals)
                opp_dict = dc_asdict(opp)
                opp_dict["sources"] = ["Merx"]
                opportunities.append(opp_dict)
                time.sleep(1)

        finally:
            driver.quit()

        log.info(f"Merx scan done: {len(opportunities)} opportunities")
        return opportunities

    except Exception as e:
        log.error(f"Merx scan failed: {e}")
        return []


async def run_biddingo_async(capabilities, signals, search_profiles, known_clients) -> list:
    """Run Biddingo scanner (Playwright async)."""
    try:
        from biddingo_driver import run_biddingo_scan
        log.info("=== Running Biddingo scanner ===")
        return await run_biddingo_scan(
            search_profiles=search_profiles,
            capabilities=capabilities,
            signals=signals,
            known_clients=known_clients,
            min_days=MIN_DAYS,
            max_results=MAX_RESULTS,
        )
    except Exception as e:
        log.error(f"Biddingo scan failed: {e}")
        return []


# ── Cross-platform dedup & merge ──────────────────────────────────────────────
def merge_opportunities(all_opps: list) -> list:
    """
    Merge opportunities from multiple platforms.
    If same opportunity appears on 2+ platforms:
    - Keep one record
    - Set sources = ["Merx", "Biddingo"] (or whatever)
    - Add +5 to score
    Matching key: solicitation_number (if available) or title similarity.
    """
    merged = {}

    for opp in all_opps:
        # Build a dedup key — prefer solicitation number, fall back to title
        sol_num = (opp.get("solicitation_number") or "").strip().upper()
        title   = (opp.get("title") or "").strip().lower()[:60]
        key     = sol_num if len(sol_num) > 4 else title

        if not key:
            merged[id(opp)] = opp  # no key, keep as-is
            continue

        if key in merged:
            # Merge sources
            existing = merged[key]
            existing_sources = existing.get("sources", [])
            new_sources = opp.get("sources", [])
            combined = list(set(existing_sources + new_sources))
            existing["sources"] = combined
            # Boost score +5 for multi-platform
            if len(combined) > 1:
                existing["score"] = min(100, existing.get("score", 0) + 5)
            log.info(f"Merged cross-platform: {key} ({combined})")
        else:
            merged[key] = opp

    result = list(merged.values())
    log.info(f"After merge: {len(all_opps)} → {len(result)} opportunities")
    return result


# ── Email digest ──────────────────────────────────────────────────────────────
def send_digest(opportunities: list, nextcloud_url: str):
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    smtp_host  = os.getenv("SMTP_HOST",   "mail.alleyneinc.net")
    smtp_user  = os.getenv("SMTP_USER",   "tzvorygina@alleyneinc.net")
    smtp_pass  = os.getenv("SMTP_PASSWORD", "")
    digest_to  = os.getenv("DIGEST_TO",   "joel.alleyne1@gmail.com")

    if not smtp_pass:
        log.warning("SMTP_PASSWORD not set — skipping digest")
        return

    strong   = [o for o in opportunities if o.get("score", 0) >= 60]
    possible = [o for o in opportunities if 35 <= o.get("score", 0) < 60]
    multi    = [o for o in opportunities if len(o.get("sources", [])) > 1]

    platforms_run = sorted(set(s for o in opportunities for s in o.get("sources", [])))
    scan_date = datetime.now().strftime("%Y-%m-%d %H:%M")

    body = f"""
ALLEYNE GROUP — DAILY BID SCAN
Scan date: {scan_date}
Platforms scanned: {", ".join(platforms_run)}

SUMMARY
────────────────────────────────────
Total opportunities: {len(opportunities)}
Strong fits (60+):   {len(strong)}
Possible fits (35+): {len(possible)}
Found on 2+ platforms: {len(multi)} (+5 score bonus applied)

View master tracker:
{nextcloud_url}

TOP OPPORTUNITIES
════════════════════════════════════════════
"""
    for opp in sorted(opportunities, key=lambda x: x.get("score", 0), reverse=True)[:10]:
        sources_str = " + ".join(opp.get("sources", ["?"]))
        body += f"""
[{opp.get('score', 0):3d}] {opp.get('recommendation', '')}  [{sources_str}]
  {opp.get('title', '')}
  {opp.get('organization', '')}
  Closes: {opp.get('closing_date', '')} ({opp.get('days_to_close', '?')} days)
  {opp.get('url', '')}
"""

    msg = MIMEMultipart()
    msg["From"]    = smtp_user
    msg["To"]      = digest_to
    msg["Subject"] = f"Daily Bid Scan — {len(strong)} strong fits — {scan_date}"
    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP_SSL(smtp_host, 465) as server:
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        log.info(f"Digest sent to {digest_to}")
    except Exception as e:
        log.error(f"Digest email failed: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────
async def main_async():
    log.info(f"Starting bid scan — platforms: {PLATFORMS}")

    capabilities, signals, search_profiles = load_criteria(COMPANY)
    known_clients = load_known_clients()

    if not search_profiles:
        log.warning("No search profiles found — cannot scan")
        return

    all_opportunities = []

    # Run Merx (sync, Selenium)
    if "Merx" in PLATFORMS:
        merx_opps = run_merx(capabilities, signals, search_profiles, known_clients)
        all_opportunities.extend(merx_opps)

    # Run Biddingo (async, Playwright)
    if "Biddingo" in PLATFORMS:
        biddingo_opps = await run_biddingo_async(capabilities, signals, search_profiles, known_clients)
        all_opportunities.extend(biddingo_opps)

    # Future platforms — add here:
    # if "AMCI" in PLATFORMS:
    #     from amci_driver import run_amci_scan
    #     all_opportunities.extend(await run_amci_scan(...))

    if not all_opportunities:
        log.warning("No opportunities found across all platforms")
        return

    # Merge cross-platform duplicates
    merged = merge_opportunities(all_opportunities)

    # Write to master Nextcloud file
    from nextcloud_writer import write_to_nextcloud
    nextcloud_url = write_to_nextcloud(
        opportunities=merged,
        known_clients=known_clients,
        company=COMPANY,
        platform="+".join(PLATFORMS),  # e.g. "Merx+Biddingo"
        profile="All-Profiles",
    )

    log.info(f"Master tracker updated: {nextcloud_url}")

    # Send digest
    send_digest(merged, nextcloud_url)

    log.info(f"Scan complete! {len(merged)} total opportunities")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
