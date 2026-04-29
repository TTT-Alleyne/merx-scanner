"""
AMCI Scanner — Alleyne Group
Scrapes RFPs from AMC Institute portal (https://rfp.amcinstitute.org).

Cloudflare Turnstile blocks headless login — solution is manual login:
  1. Scanner opens a visible browser window at rfp.amcinstitute.org
  2. You log in manually (takes ~30 seconds)
  3. You press Enter in the terminal
  4. Scanner extracts session cookies automatically and runs the scan

No cookie copying, no DevTools, no .env editing needed.
Cookie is cached in /results/amci_cookies.json and reused until it expires (~30 days).
"""

import asyncio
import json
import logging
import os
import re
import requests
from datetime import datetime, date
from pathlib import Path

log = logging.getLogger(__name__)

RFP_API_URL   = "https://rfp.amcinstitute.org/api/rfps"
RFP_BASE_URL  = "https://rfp.amcinstitute.org"
RFP_LIST_URL  = "https://rfp.amcinstitute.org/rfps"
COOKIE_CACHE  = os.getenv("AMCI_COOKIE_CACHE", "/results/amci_cookies.json")


# ── Helpers ───────────────────────────────────────────────────────────────────
def parse_days_left(deadline_str: str) -> int:
    if not deadline_str:
        return 0
    for fmt in ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%d", "%m/%d/%Y", "%B %d, %Y"]:
        try:
            dt = datetime.strptime(deadline_str.strip()[:26], fmt)
            return max(0, (dt.date() - date.today()).days)
        except Exception:
            continue
    return 0


def parse_date_str(date_str: str) -> str:
    if not date_str:
        return ""
    for fmt in ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"]:
        try:
            dt = datetime.strptime(date_str.strip()[:26], fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            continue
    return date_str[:10] if len(date_str) >= 10 else date_str


# ── Cookie cache ──────────────────────────────────────────────────────────────
def load_cached_cookies() -> str:
    """Load cookies from cache file if it exists and is recent enough."""
    try:
        path = Path(COOKIE_CACHE)
        if not path.exists():
            return ""
        data = json.loads(path.read_text())
        saved_date = data.get("saved_date", "")
        cookies    = data.get("cookies", "")
        if not cookies or not saved_date:
            return ""
        # Check if cookie is older than 25 days (expire before the ~30 day limit)
        saved = datetime.strptime(saved_date, "%Y-%m-%d").date()
        age   = (date.today() - saved).days
        if age > 25:
            log.info(f"Cached AMCI cookies are {age} days old — need fresh login")
            return ""
        log.info(f"Using cached AMCI cookies (saved {saved_date}, {age} days old)")
        return cookies
    except Exception as e:
        log.warning(f"Could not load cookie cache: {e}")
        return ""


def save_cookies(cookie_str: str):
    """Save cookies to cache file with today's date."""
    try:
        Path(COOKIE_CACHE).parent.mkdir(parents=True, exist_ok=True)
        data = {
            "saved_date": date.today().strftime("%Y-%m-%d"),
            "cookies":    cookie_str,
        }
        Path(COOKIE_CACHE).write_text(json.dumps(data, indent=2))
        log.info(f"AMCI cookies cached to {COOKIE_CACHE}")
    except Exception as e:
        log.warning(f"Could not save cookie cache: {e}")


# ── Manual login via visible browser ─────────────────────────────────────────
async def get_cookies_via_manual_login() -> str:
    """
    Opens a visible browser at rfp.amcinstitute.org.
    Waits for user to log in manually, then extracts Bearer token + cookies.
    Returns auth string ready for HTTP headers.
    """
    from playwright.async_api import async_playwright

    log.info("=" * 60)
    log.info("AMCI MANUAL LOGIN REQUIRED")
    log.info("=" * 60)
    log.info("A browser window will open at rfp.amcinstitute.org")
    log.info("Please log in manually, then come back here and press Enter")
    log.info("=" * 60)

    auth_str = ""

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=False,
            args=["--window-size=1280,800"]
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        )
        page = await context.new_page()

        # Intercept API calls to grab the Bearer token automatically
        bearer_token = {"value": ""}

        async def handle_request(request):
            auth = request.headers.get("authorization", "")
            if auth.startswith("Bearer ") and not bearer_token["value"]:
                bearer_token["value"] = auth
                log.info(f"Bearer token captured automatically!")

        page.on("request", handle_request)

        await page.goto(RFP_LIST_URL, wait_until="domcontentloaded")
        log.info(f"Browser opened at: {page.url}")

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: input("\n>>> Press Enter after you have logged in to rfp.amcinstitute.org <<<\n")
        )

        # Try to get token from intercepted requests first
        if bearer_token["value"]:
            auth_str = bearer_token["value"]
            log.info("Using intercepted Bearer token")
        else:
            # Try extracting from localStorage / sessionStorage
            token = await page.evaluate("""() => {
                for (let k of Object.keys(localStorage)) {
                    const v = localStorage.getItem(k);
                    if (v && v.startsWith('Bearer ')) return v;
                    if (v && v.match(/^[0-9]+\|[A-Za-z0-9]{20,}/)) return 'Bearer ' + v;
                }
                for (let k of Object.keys(sessionStorage)) {
                    const v = sessionStorage.getItem(k);
                    if (v && v.startsWith('Bearer ')) return v;
                    if (v && v.match(/^[0-9]+\|[A-Za-z0-9]{20,}/)) return 'Bearer ' + v;
                }
                return '';
            }""")
            if token:
                auth_str = token
                log.info("Using Bearer token from browser storage")

        if not auth_str:
            # Fall back to XSRF token + cookies
            cookies = await context.cookies()
            xsrf = next((c["value"] for c in cookies if c["name"] == "XSRF-TOKEN"), "")
            cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
            auth_str = f"XSRF:{xsrf}||COOKIES:{cookie_str}"
            log.info("Falling back to XSRF + cookies")

        await browser.close()

    return auth_str


# ── API fetch with cookies ────────────────────────────────────────────────────
def fetch_rfps(session_cookie: str) -> list:
    """Call AMCI API using Bearer token or cookies."""
    headers = {
        "User-Agent":       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept":           "application/json, text/plain, */*",
        "Referer":          RFP_LIST_URL,
        "X-Requested-With": "XMLHttpRequest",
    }

    # Bearer token (primary method)
    if session_cookie.startswith("Bearer "):
        headers["Authorization"] = session_cookie
    # XSRF + cookies fallback
    elif session_cookie.startswith("XSRF:"):
        parts = session_cookie.split("||COOKIES:")
        xsrf = parts[0].replace("XSRF:", "")
        cookies = parts[1] if len(parts) > 1 else ""
        headers["Cookie"] = cookies
        headers["X-XSRF-TOKEN"] = xsrf
    else:
        headers["Cookie"] = session_cookie

    try:
        import urllib.parse
        t = int(__import__("time").time() * 1000)
        r = requests.get(
            f"{RFP_API_URL}?_t={t}&offset=0&limit=200",
            headers=headers,
            timeout=30
        )
        log.info(f"API status: {r.status_code}, body length: {len(r.text)}")

        if r.status_code in (401, 403):
            log.error("Auth failed — token expired or invalid")
            return []
        if r.status_code != 200:
            log.warning(f"API returned {r.status_code}: {r.text[:300]}")
            return []

        data = r.json()
        rfps = data if isinstance(data, list) else (
            data.get("data") or data.get("rfps") or
            data.get("results") or data.get("items") or []
        )
        log.info(f"API returned {len(rfps)} RFPs")
        if rfps:
            log.info(f"API fields: {list(rfps[0].keys())}")
        return rfps

    except Exception as e:
        log.error(f"API call failed: {e}")
        return []


def fetch_rfp_detail(rfp_id, session_cookie: str) -> dict:
    """Fetch single RFP detail via API."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept":     "application/json, text/plain, */*",
        "Referer":    f"{RFP_BASE_URL}/rfps/{rfp_id}",
    }
    if session_cookie.startswith("Bearer "):
        headers["Authorization"] = session_cookie
    else:
        headers["Cookie"] = session_cookie

    try:
        r = requests.get(f"{RFP_API_URL}/{rfp_id}", headers=headers, timeout=15)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        log.warning(f"Detail fetch failed for {rfp_id}: {e}")
    return {}


# ── Map API fields → standard opportunity format ──────────────────────────────
def map_rfp_to_opportunity(rfp: dict, detail: dict = None) -> dict:
    d = {**rfp, **(detail or {})}

    title          = (d.get("rfp_title") or d.get("title") or d.get("name") or "")
    org            = (d.get("organization_name") or d.get("organization") or d.get("org_name") or "")
    description    = (d.get("executive_summary") or d.get("description") or d.get("summary") or "")
    deadline       = (d.get("proposal_deadline") or d.get("deadline") or d.get("closing_date") or "")
    start_date     = (d.get("startdate_services") or d.get("start_date") or d.get("created_at") or "")
    location       = (d.get("geographical_location") or d.get("location") or "")
    contact_name   = (d.get("contact_name") or d.get("contact") or "")
    contact_email  = (d.get("contact_email") or d.get("email") or "")
    rfp_id         = str(d.get("id") or d.get("rfp_id") or "")
    amc_pref       = (d.get("accredited_amc") or d.get("amc_preference") or
                      d.get("accredited_amc_preference") or "")
    mgmt_structure = (d.get("current_management_structure") or d.get("management_structure") or "")
    membership_sw  = (d.get("membership_software") or "")

    if not title and org:
        title = f"AMC Management RFP — {org}"

    extra_info = []
    if mgmt_structure:
        extra_info.append(f"Current structure: {mgmt_structure}")
    if membership_sw:
        extra_info.append(f"Membership software: {membership_sw}")
    if amc_pref:
        extra_info.append(f"AMC preference: {amc_pref}")
    if extra_info:
        description = (description + " | " + " | ".join(extra_info)).strip(" |")

    return {
        "title":                title,
        "organization":         org,
        "issuing_org":          org,
        "solicitation_number":  rfp_id,
        "reference_number":     rfp_id,
        "solicitation_type":    "RFI" if "rfi" in title.lower() else "RFP",
        "closing_date":         parse_date_str(deadline),
        "days_to_close":        parse_days_left(deadline),
        "published_date":       parse_date_str(start_date),
        "location":             location,
        "description":          description[:500],
        "contact_name":         contact_name,
        "contact_email":        contact_email,
        "url":                  f"{RFP_BASE_URL}/rfps/{rfp_id}" if rfp_id else RFP_LIST_URL,
        "agreement_types":      [],
        "contract_duration":    "",
        "bid_intent":           "",
        "bid_submission_type":  amc_pref,
        "qa_deadline":          "",
        "score":                0,
        "matched_capabilities": [],
        "matched_signals":      [],
        "sources":              ["AMCI"],
        "profile":              "All-Profiles",
        "platform_id":          f"AMCI:{rfp_id}" if rfp_id else "",
        "recommendation":       "",
    }


# ── Main scanner function ─────────────────────────────────────────────────────
async def run_amci_scan(
    search_profiles: list,
    capabilities: list,
    signals: list,
    known_clients: list,
    min_days: int = 0,
    max_results: int = 200,
) -> list:
    """
    Main entry point.
    1. Tries cached cookies first (valid for ~25 days)
    2. If no valid cache, opens visible browser for manual login
    3. Extracts cookies automatically after you press Enter
    4. Runs the scan
    """
    from scorer import score_opportunity_dict

    log.info("=== Running AMCI scanner ===")

    # Step 1: try cached cookies
    session_cookie = load_cached_cookies()

    # Step 2: if no valid cache, do manual login
    if not session_cookie:
        session_cookie = await get_cookies_via_manual_login()
        if not session_cookie:
            log.error("AMCI scan aborted — could not get session cookies")
            return []
        save_cookies(session_cookie)

    # Step 3: verify cookies work by calling API
    rfps = fetch_rfps(session_cookie)

    # Step 4: if cookies expired mid-session, retry with fresh login
    if not rfps:
        log.info("Cached cookies appear expired — requesting fresh login")
        session_cookie = await get_cookies_via_manual_login()
        if not session_cookie:
            log.error("AMCI scan aborted — login failed")
            return []
        save_cookies(session_cookie)
        rfps = fetch_rfps(session_cookie)

    if not rfps:
        log.error("AMCI: no RFPs returned from API")
        return []

    log.info(f"AMCI: Processing {min(len(rfps), max_results)} RFPs")
    all_opportunities = []

    for i, rfp in enumerate(rfps[:max_results], 1):
        rfp_id = rfp.get("id") or rfp.get("rfp_id")
        detail = {}
        if rfp_id:
            log.info(f"AMCI {i}/{min(len(rfps), max_results)}: detail id={rfp_id}")
            detail = fetch_rfp_detail(rfp_id, session_cookie)
            await asyncio.sleep(0.3)

        mapped = map_rfp_to_opportunity(rfp, detail)

        # Skip closed/cancelled
        status_raw = (rfp.get("status") or "").lower()
        if status_raw in ("closed", "finalized", "cancelled"):
            log.info(f"  Skipped ({status_raw}): {mapped['title'][:50]}")
            continue

        # Filter by days
        days = mapped.get("days_to_close", 0)
        if min_days > 0 and 0 < days < min_days:
            log.info(f"  Skipped — {days} days: {mapped['title'][:50]}")
            continue

        # Score
        mapped = score_opportunity_dict(mapped, capabilities, signals)

        # AMCI = always association management — Alleyne's core service
        mapped["score"] = min(100, mapped["score"] + 15)
        caps = mapped.get("matched_capabilities", [])
        if not any("association" in c.lower() for c in caps):
            caps.insert(0, "Association management consulting")
            mapped["matched_capabilities"] = caps[:5]

        log.info(f"  [{mapped['score']:3d}] {mapped['title'][:60]}")
        all_opportunities.append(mapped)

    log.info(f"AMCI scan complete: {len(all_opportunities)} opportunities")
    return all_opportunities