"""
Bonfire Scanner — Alleyne Group
Scrapes public Bonfire procurement portals (no login required).
Portals: UWO, TMU, Barbados Government — add more to PORTALS list.
Uses Playwright async. Scoring via shared scorer.py.

Column layouts differ by portal:
  UWO/TMU:  Status | Ref# | Project | Close Date | Days Left | Action
  Barbados: Status | Ref# | Project | Department | Close Date | Days Left | Action

min_days controlled via BONFIRE_MIN_DAYS env var in docker-compose.
"""

import asyncio
import logging
import re
from datetime import date

from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeoutError

log = logging.getLogger(__name__)

# ── Portal definitions ────────────────────────────────────────────────────────
# has_department=True means portal has an extra Department column before Close Date
PORTALS = [
    {
        "id":             "UWO",
        "name":           "Western University",
        "base_url":       "https://uwo.bonfirehub.ca",
        "list_url":       "https://uwo.bonfirehub.ca/portal/?tab=openOpportunities",
        "has_department": False,
    },
    {
        "id":             "TMU",
        "name":           "Toronto Metropolitan University",
        "base_url":       "https://tmu.bonfirehub.ca",
        "list_url":       "https://tmu.bonfirehub.ca/portal/?tab=openOpportunities",
        "has_department": False,
    },
    {
        "id":             "GovBB",
        "name":           "Government of Barbados",
        "base_url":       "https://gov-bb.bonfirehub.com",
        "list_url":       "https://gov-bb.bonfirehub.com/portal/?tab=openOpportunities",
        "has_department": True,   # has Department column between Project and Close Date
    },
]


# ── Helpers ───────────────────────────────────────────────────────────────────
def parse_days_left(text: str) -> int:
    try:
        m = re.search(r'\d+', str(text or ""))
        return int(m.group()) if m else 0
    except Exception:
        return 0


def parse_close_date(text: str) -> str:
    if not text:
        return ""
    return re.sub(r'\s+(EDT|EST|ET|UTC|AST|ADT)$', '', text.strip())


# ── Playwright scraper ────────────────────────────────────────────────────────
async def get_opportunity_list(page: Page, portal: dict) -> list:
    """Extract all OPEN opportunity rows from a Bonfire portal list page."""
    log.info(f"[{portal['id']}] Loading: {portal['list_url']}")
    try:
        await page.goto(portal["list_url"], wait_until="networkidle", timeout=60000)
    except PlaywrightTimeoutError:
        log.warning(f"[{portal['id']}] Page load timed out, trying anyway")

    try:
        await page.wait_for_selector("table tbody tr", timeout=20000)
    except PlaywrightTimeoutError:
        log.warning(f"[{portal['id']}] No table found — portal may be empty or blocked")
        return []

    rows = await page.query_selector_all("table tbody tr")
    opps = []
    has_dept = portal.get("has_department", False)

    for row in rows:
        try:
            cells = await row.query_selector_all("td")
            if len(cells) < 5:
                continue

            status = (await cells[0].inner_text()).strip()
            ref_num = (await cells[1].inner_text()).strip()
            title   = (await cells[2].inner_text()).strip()

            if has_dept:
                # Status | Ref# | Project | Department | Close Date | Days Left | Action
                organization = (await cells[3].inner_text()).strip()
                close_date   = (await cells[4].inner_text()).strip()
                days_left    = (await cells[5].inner_text()).strip() if len(cells) > 5 else "0"
            else:
                # Status | Ref# | Project | Close Date | Days Left | Action
                organization = portal["name"]  # use portal name as org
                close_date   = (await cells[3].inner_text()).strip()
                days_left    = (await cells[4].inner_text()).strip() if len(cells) > 4 else "0"

            link_el    = await row.query_selector("a")
            detail_url = await link_el.get_attribute("href") if link_el else ""
            if detail_url and not detail_url.startswith("http"):
                detail_url = portal["base_url"] + detail_url

            platform_id = ""
            m = re.search(r'/opportunities/(\d+)', detail_url or "")
            if m:
                platform_id = f"Bonfire-{portal['id']}:{m.group(1)}"

            if status.upper() != "OPEN":
                continue

            opps.append({
                "ref_num":       ref_num,
                "title":         title,
                "organization":  organization,
                "close_date":    parse_close_date(close_date),
                "days_to_close": parse_days_left(days_left),
                "detail_url":    detail_url,
                "platform_id":   platform_id,
                "portal_id":     portal["id"],
                "portal_name":   portal["name"],
            })
        except Exception as e:
            log.warning(f"[{portal['id']}] Row parse error: {e}")

    log.info(f"[{portal['id']}] Found {len(opps)} open opportunities")
    return opps


async def get_opportunity_detail(page: Page, opp: dict, portal: dict) -> dict:
    """Load detail page and extract additional fields."""
    url = opp.get("detail_url", "")
    if not url:
        return opp

    try:
        await page.goto(url, wait_until="networkidle", timeout=30000)
    except PlaywrightTimeoutError:
        log.warning(f"[{portal['id']}] Detail timeout: {url}")
        return opp

    try:
        text = await page.inner_text("body")

        m = re.search(r'Type:\s*([^\n]+)', text)
        opp["solicitation_type"] = m.group(1).strip() if m else ""

        m = re.search(r'Open Date:\s*([^\n]+)', text)
        opp["published_date"] = m.group(1).strip() if m else ""

        m = re.search(r'Questions Due Date:\s*([^\n]+)', text)
        opp["qa_deadline"] = m.group(1).strip() if m else ""

        m = re.search(r'Project Description:\s*\n([^\n]+(?:\n[^\n]+){0,5})', text)
        if m:
            desc = m.group(1).strip()
            opp["description"] = "" if "refer to tender notice" in desc.lower() else desc
        else:
            opp["description"] = ""

        commodity_matches = re.findall(r'UNSPSC\s+\d+\s+([^\n:]+)', text)
        opp["commodity_codes"] = "; ".join(commodity_matches[:3]) if commodity_matches else ""

    except Exception as e:
        log.warning(f"[{portal['id']}] Detail parse error {url}: {e}")

    return opp


# ── Main scanner function ─────────────────────────────────────────────────────
async def run_bonfire_scan(
    search_profiles: list,
    capabilities: list,
    signals: list,
    known_clients: list,
    min_days: int = 3,
    max_results: int = 200,
) -> list:
    """
    Main entry point. Scans all portals in PORTALS list.
    min_days: passed from runner.py via BONFIRE_MIN_DAYS env var.
    Returns list compatible with nextcloud_writer.py.
    """
    from scorer import score_opportunity_dict

    all_opportunities = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ))
        page = await context.new_page()

        for portal in PORTALS:
            log.info(f"=== Scanning {portal['name']} ({portal['id']}) ===")
            try:
                opps = await get_opportunity_list(page, portal)
                opps = [o for o in opps if o.get("days_to_close", 0) >= min_days]
                log.info(f"[{portal['id']}] {len(opps)} opps after min_days={min_days} filter")
                opps = opps[:max_results]

                for i, opp in enumerate(opps, 1):
                    log.info(f"[{portal['id']}] Detail {i}/{len(opps)}: {opp.get('title', '')[:60]}")
                    opp = await get_opportunity_detail(page, opp, portal)
                    await asyncio.sleep(0.5)

                for opp in opps:
                    # Use organization from Department column (Barbados) or portal name (UWO/TMU)
                    org = opp.get("organization") or opp.get("portal_name", portal["name"])

                    mapped = {
                        "title":                opp.get("title", ""),
                        "organization":         org,
                        "issuing_org":          org,
                        "solicitation_number":  opp.get("ref_num", ""),
                        "reference_number":     opp.get("ref_num", ""),
                        "solicitation_type":    opp.get("solicitation_type", ""),
                        "closing_date":         opp.get("close_date", ""),
                        "days_to_close":        opp.get("days_to_close", 0),
                        "published_date":       opp.get("published_date", ""),
                        "location":             f"{opp.get('portal_name', '')}, {portal.get('id', '')}",
                        "description":          opp.get("description", "") or opp.get("commodity_codes", ""),
                        "commodity_codes":      opp.get("commodity_codes", ""),
                        "contact_name":         "",
                        "contact_email":        "",
                        "url":                  opp.get("detail_url", ""),
                        "agreement_types":      [],
                        "contract_duration":    "",
                        "bid_intent":           "",
                        "bid_submission_type":  "",
                        "qa_deadline":          opp.get("qa_deadline", ""),
                        "score":                0,
                        "matched_capabilities": [],
                        "matched_signals":      [],
                        "sources":              ["Bonfire"],
                        "profile":              "All-Profiles",
                        "platform_id":          opp.get("platform_id", ""),
                        "recommendation":       "",
                    }
                    mapped = score_opportunity_dict(mapped, capabilities, signals)
                    all_opportunities.append(mapped)

                log.info(f"[{portal['id']}] Done: {len(opps)} opportunities")

            except Exception as e:
                log.error(f"[{portal['id']}] Failed: {e}")

        await browser.close()

    log.info(f"Bonfire scan complete: {len(all_opportunities)} total")
    return all_opportunities


# ── Standalone test ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    import json, os
    from dotenv import load_dotenv
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    criteria_file = os.getenv("CRITERIA_FILE", "/data/criteria_sets.json")
    try:
        with open(criteria_file) as f:
            data = json.load(f)
        caps, sigs, profiles = [], [], []
        for s in data["sets"]:
            if s["company"] != "Alleyne Inc.":
                continue
            active = [i for i in s["items"] if i.get("active", True)]
            if s["type"] == "capabilities": caps = active
            elif s["type"] == "sales":      sigs = active
            elif s["type"] == "search":     profiles = active
    except Exception as e:
        log.warning(f"Could not load criteria: {e}")
        caps, sigs, profiles = [], [], []

    results = asyncio.run(run_bonfire_scan(
        search_profiles=profiles,
        capabilities=caps,
        signals=sigs,
        known_clients=[],
        min_days=3,
    ))

    print(f"\nTotal: {len(results)} opportunities")
    for r in sorted(results, key=lambda x: x.get("score", 0), reverse=True)[:10]:
        print(f"  [{r['score']:3d}] {r['title'][:60]} — {r['organization']}")