"""
Runner — Alleyne Group Bid Scanner
Runs all enabled platform scanners, writes daily files per profile.
Combiner runs as separate Docker service after all scanners complete.
Supports: Merx (Selenium), Biddingo (Playwright), Bonfire (Playwright)
AMCI: parked — Cloudflare Turnstile blocks headless login.

Profile filtering:
  Each opportunity is matched against profile include_keywords.
  Only matching opportunities go into that profile's daily file.
  Merx already filters server-side via search queries.
  Biddingo and Bonfire filter client-side here.

FIXES 2026-04-29:
  - Bug 5: run_merx() sets platform_id = f"Merx:{merx_id}" on each opp dict
"""

import os
import re
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
CRITERIA_FILE    = os.getenv("CRITERIA_FILE",    "/data/criteria_sets.json")
KNOWN_CLIENTS    = os.getenv("KNOWN_CLIENTS",    "/app/known_clients.json")
COMPANY          = os.getenv("COMPANY",          "Alleyne Inc.")
MIN_DAYS         = int(os.getenv("MIN_DAYS_TO_BID", "21"))
MAX_RESULTS      = int(os.getenv("MAX_RESULTS",  "50"))
BONFIRE_MIN_DAYS = int(os.getenv("BONFIRE_MIN_DAYS", "3"))

_platforms_env = os.getenv("PLATFORMS") or os.getenv("PLATFORM") or "Merx,Biddingo"
PLATFORMS = [p.strip() for p in _platforms_env.split(",") if p.strip()]


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


# ── Profile keyword filtering ─────────────────────────────────────────────────
def extract_profile_keywords(profile: dict) -> list:
    raw = profile.get("include_keywords", "")
    raw = re.sub(r'NOT\s*\([^)]+\)', '', raw, flags=re.IGNORECASE)
    raw = re.sub(r'\b(OR|AND|NOT)\b', ' ', raw, flags=re.IGNORECASE)
    raw = re.sub(r'[()"\']', ' ', raw)
    keywords = [w.strip().lower() for w in raw.split() if len(w.strip()) > 4]
    return list(set(keywords))


def opportunity_matches_profile(opp: dict, profile: dict) -> bool:
    keywords = extract_profile_keywords(profile)
    if not keywords:
        return True
    text = " ".join([
        (opp.get("title") or ""),
        (opp.get("description") or ""),
        (opp.get("organization") or ""),
        (opp.get("solicitation_type") or ""),
        (opp.get("location") or ""),
    ]).lower()
    return any(kw in text for kw in keywords)


def filter_opps_for_profile(opportunities: list, profile: dict) -> list:
    matched = [o for o in opportunities if opportunity_matches_profile(o, profile)]
    log.info(f"Profile '{profile.get('profile_name')}': "
             f"{len(matched)}/{len(opportunities)} opportunities match")
    return matched


# ── Platform runners ──────────────────────────────────────────────────────────
def run_merx(capabilities, signals, search_profiles, known_clients) -> list:
    """Merx filters server-side via search queries — no client-side filtering needed."""
    try:
        from scanner import (make_driver, login, search_opportunities,
                             extract_opportunity, score_opportunity)
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
                opp_dict["profile"] = "All-Profiles"
                # FIX Bug 5: set platform_id so combiner can track Merx rows across runs
                opp_dict["platform_id"] = f"Merx:{opp.merx_id}"
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


async def run_bonfire_async(capabilities, signals, search_profiles, known_clients) -> list:
    try:
        from bonfire_driver import run_bonfire_scan
        log.info("=== Running Bonfire scanner ===")
        return await run_bonfire_scan(
            search_profiles=search_profiles,
            capabilities=capabilities,
            signals=signals,
            known_clients=known_clients,
            min_days=BONFIRE_MIN_DAYS,
            max_results=MAX_RESULTS,
        )
    except Exception as e:
        log.error(f"Bonfire scan failed: {e}")
        return []


# ── Write daily files with profile filtering ──────────────────────────────────
def write_daily_files(opportunities: list, search_profiles: list,
                      known_clients: list, platform: str,
                      apply_profile_filter: bool = True):
    from nextcloud_writer import write_to_nextcloud

    active_profiles = [p for p in search_profiles if p.get("active", True)]
    files_written = 0

    for profile in active_profiles:
        profile_name = profile.get("profile_name", "Default")

        if apply_profile_filter:
            profile_opps = filter_opps_for_profile(opportunities, profile)
        else:
            profile_opps = opportunities

        if not profile_opps:
            log.info(f"  [{platform}] No opportunities for profile '{profile_name}' — skipping file")
            continue

        try:
            write_to_nextcloud(
                opportunities=profile_opps,
                known_clients=known_clients,
                company=COMPANY,
                profile_name=profile_name,
                platform=platform
            )
            files_written += 1
        except Exception as e:
            log.error(f"Failed to write {platform} daily file for '{profile_name}': {e}")

    log.info(f"{platform} daily files written: {files_written}/{len(active_profiles)} profiles")
    return files_written


# ── Cross-platform dedup & merge ──────────────────────────────────────────────
def merge_opportunities(all_opps: list) -> list:
    merged = {}
    for opp in all_opps:
        sol_num = (opp.get("solicitation_number") or "").strip().upper()
        title   = (opp.get("title") or "").strip().lower()[:60]
        key     = sol_num if len(sol_num) > 4 else title
        if not key:
            merged[id(opp)] = opp
            continue
        if key in merged:
            existing = merged[key]
            combined = list(set(existing.get("sources", []) + opp.get("sources", [])))
            existing["sources"] = combined
            if len(combined) > 1:
                existing["score"] = min(100, existing.get("score", 0) + 5)
            log.info(f"Merged cross-platform: {key} ({combined})")
        else:
            merged[key] = opp
    result = list(merged.values())
    log.info(f"After merge: {len(all_opps)} → {len(result)} opportunities")
    return result


# ── Email digest ──────────────────────────────────────────────────────────────
def send_digest(opportunities: list):
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    smtp_host = os.getenv("SMTP_HOST",     "mail.alleyneinc.net")
    smtp_user = os.getenv("SMTP_USER",     "tzvorygina@alleyneinc.net")
    smtp_pass = os.getenv("SMTP_PASSWORD", "")
    digest_to = os.getenv("DIGEST_TO",     "joel.alleyne1@gmail.com")

    if not smtp_pass:
        log.warning("SMTP_PASSWORD not set — skipping digest")
        return

    strong        = [o for o in opportunities if o.get("score", 0) >= 60]
    possible      = [o for o in opportunities if 35 <= o.get("score", 0) < 60]
    multi         = [o for o in opportunities if len(o.get("sources", [])) > 1]
    platforms_run = sorted(set(s for o in opportunities for s in o.get("sources", [])))
    scan_date     = datetime.now().strftime("%Y-%m-%d %H:%M")

    nextcloud_url = (
        f"https://cloud.alleyneinc.net/remote.php/dav/files/"
        f"{os.getenv('NEXTCLOUD_USER', 'tzvorygina')}/"
        f"{os.getenv('NEXTCLOUD_FOLDER', 'Alleyne Inc/AlleyneAdmAgent')}/"
        f"AlleyneInc_BidTracker.xlsx"
    )

    body = f"""
ALLEYNE GROUP — DAILY BID SCAN
Scan date: {scan_date}
Platforms scanned: {", ".join(platforms_run)}

SUMMARY
────────────────────────────────────
Total opportunities: {len(opportunities)}
Strong fits (60+):   {len(strong)}
Possible fits (35+): {len(possible)}
Found on 2+ platforms: {len(multi)}

View master tracker: {nextcloud_url}

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

    if "Merx" in PLATFORMS:
        merx_opps = run_merx(capabilities, signals, search_profiles, known_clients)
        all_opportunities.extend(merx_opps)
        if merx_opps:
            write_daily_files(merx_opps, search_profiles, known_clients,
                              platform="MerxS", apply_profile_filter=False)
        else:
            log.warning("Merx returned 0 opportunities — skipping daily file write")

    if "Biddingo" in PLATFORMS:
        biddingo_opps = await run_biddingo_async(capabilities, signals, search_profiles, known_clients)
        all_opportunities.extend(biddingo_opps)
        if biddingo_opps:
            write_daily_files(biddingo_opps, search_profiles, known_clients,
                              platform="BidDS", apply_profile_filter=True)
        else:
            log.warning("Biddingo returned 0 opportunities — skipping daily file write")

    if "Bonfire" in PLATFORMS:
        bonfire_opps = await run_bonfire_async(capabilities, signals, search_profiles, known_clients)
        all_opportunities.extend(bonfire_opps)
        if bonfire_opps:
            write_daily_files(bonfire_opps, search_profiles, known_clients,
                              platform="BonfireS", apply_profile_filter=True)
        else:
            log.warning("Bonfire returned 0 opportunities — skipping daily file write")

    # Future: AMCI parked — Cloudflare Turnstile blocks headless login
    # if "AMCI" in PLATFORMS:
    #     from amci_driver import run_amci_scan
    #     amci_opps = await run_amci_async(...)

    if not all_opportunities:
        log.warning("No opportunities found across all platforms")
        return

    merged = merge_opportunities(all_opportunities)
    send_digest(merged)
    log.info(f"Scan complete! {len(merged)} total opportunities")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()