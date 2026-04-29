"""
Biddingo scanner driver — Alleyne Group.
Uses Playwright (async). Handles login, search, and opportunity extraction.
Called by runner.py — does NOT run standalone.
Scoring via shared scorer.py module.

FIXES 2026-04-29:
  - Bug 1+2: platform_id now correctly extracted from URL and set in opp dict
  - Bug 3: description falls back to body_text when no CSS selector matches
"""

import os
import re
import logging
from datetime import datetime
from typing import Optional

log = logging.getLogger(__name__)

BIDDINGO_LOGIN_URL  = "https://www.biddingo.com/login"
BIDDINGO_SEARCH_URL = "https://www.biddingo.com/search/with-my-profile"
BIDDINGO_BASE_URL   = "https://www.biddingo.com"


async def login(page):
    """Log into Biddingo with email/password."""
    email    = os.getenv("BIDDINGO_EMAIL", os.getenv("MERX_EMAIL", ""))
    password = os.getenv("BIDDINGO_PASSWORD", os.getenv("MERX_PASSWORD", ""))

    if not email or not password:
        raise ValueError("BIDDINGO_EMAIL and BIDDINGO_PASSWORD must be set in .env")

    log.info("Logging into Biddingo...")
    await page.goto(BIDDINGO_LOGIN_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(5000)
    await page.wait_for_selector("input", timeout=30000)

    inputs_info = await page.evaluate(
        "Array.from(document.querySelectorAll('input')).map((el,i)=>({i,type:el.type,name:el.name,id:el.id,placeholder:el.placeholder,visible:el.offsetParent!==null}))"
    )
    log.info(f"Biddingo inputs: {inputs_info}")

    email_field    = page.locator("#mat-input-0")
    password_field = page.locator("#mat-input-1")

    await email_field.click()
    await page.wait_for_timeout(300)
    await email_field.fill("")
    await page.keyboard.type(email, delay=100)
    await page.keyboard.press("Tab")
    await page.wait_for_timeout(500)

    await password_field.click()
    await page.wait_for_timeout(300)
    await password_field.fill("")
    await page.keyboard.type(password, delay=100)
    await page.keyboard.press("Tab")
    await page.wait_for_timeout(500)

    log.info("Filled Angular Material login form via keyboard")

    email_val = await email_field.input_value()
    pass_val  = await password_field.input_value()
    log.info(f"Email field value: '{email_val}', Password length: {len(pass_val)}")

    await page.locator("button:has-text('Sign in')").click()
    log.info("Clicked Sign in button")
    await page.wait_for_timeout(3000)
    await page.wait_for_load_state("networkidle")
    log.info(f"After login: {page.url}")

    if "login" in page.url.lower():
        raise Exception("Biddingo login failed — still on login page")

    log.info("Biddingo login successful")


async def search_opportunities(page, search_profiles: list, max_results: int = 50) -> list:
    """Run each search profile and return list of unique opportunity URLs."""
    all_urls = []

    for profile in search_profiles:
        profile_name = profile.get("profile_name", "Unknown")
        keywords     = profile.get("include_keywords", "")
        profile_max  = profile.get("max_results", max_results)

        if not keywords:
            log.warning(f"Profile '{profile_name}' has no keywords — skipping")
            continue

        log.info(f"Biddingo search: profile '{profile_name}'")
        await page.goto(BIDDINGO_SEARCH_URL)
        await page.wait_for_load_state("networkidle")

        try:
            import urllib.parse
            clean = re.sub(r'OR|AND|NOT', ' ', keywords)
            clean = re.sub(r"[()\"'()]", " ", clean)
            words = [w.strip() for w in clean.split() if len(w.strip()) > 4]
            seen, unique_words = set(), []
            for w in words:
                if w.lower() not in seen:
                    seen.add(w.lower())
                    unique_words.append(w)
            simple_keywords = " ".join(unique_words[:5])
            encoded    = urllib.parse.quote(simple_keywords)
            search_url = f"{BIDDINGO_SEARCH_URL}?k={encoded}"
            await page.goto(search_url)
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(2000)
            log.info(f"Search submitted for '{profile_name}': {search_url[:120]}")
            log.info(f"  Simplified keywords: '{simple_keywords}'")
        except Exception as e:
            log.warning(f"Search failed for '{profile_name}': {e}")
            continue

        profile_urls = []
        page_num = 1

        while len(profile_urls) < profile_max:
            await page.wait_for_timeout(3000)
            log.info(f"  Page {page_num}: scanning for View Details links...")

            view_details_links = await page.query_selector_all("a")
            clickable = []
            for link in view_details_links:
                try:
                    text = (await link.inner_text()).strip()
                    if text.lower() in ["view details", "view bid", "details"]:
                        clickable.append(link)
                except Exception:
                    continue

            log.info(f"  Found {len(clickable)} View Details links")

            page_urls = []
            num_links = len(clickable)
            for idx in range(num_links):
                try:
                    await page.wait_for_timeout(1000)
                    fresh_links = []
                    all_links = await page.query_selector_all("a")
                    for a in all_links:
                        try:
                            text = (await a.inner_text()).strip()
                            if text.lower() in ["view details", "view bid", "details"]:
                                fresh_links.append(a)
                        except Exception:
                            continue

                    if idx >= len(fresh_links):
                        break

                    await fresh_links[idx].click()
                    await page.wait_for_timeout(2000)
                    current_url = page.url
                    log.info(f"  After click URL: {current_url}")

                    if "/dashboard/bid/" in current_url and current_url not in all_urls:
                        page_urls.append(current_url)
                        log.info(f"  Got URL (navigation): {current_url}")
                        await page.go_back()
                        await page.wait_for_load_state("networkidle", timeout=10000)
                        await page.wait_for_timeout(1000)
                    else:
                        new_links = await page.evaluate(
                            "Array.from(document.querySelectorAll('a[href]')).map(a=>a.href).filter(h=>h.includes('/dashboard/bid/'))"
                        )
                        log.info(f"  Modal bid links: {new_links[:5]}")
                        for nl in new_links:
                            if nl not in all_urls and nl not in page_urls:
                                page_urls.append(nl)
                                log.info(f"  Got URL (modal): {nl}")
                        await page.keyboard.press("Escape")
                        await page.wait_for_timeout(500)
                except Exception as e:
                    log.warning(f"  Click {idx} failed: {e}")
                    try:
                        await page.go_back()
                        await page.wait_for_timeout(1000)
                    except Exception:
                        pass
                    continue

            if not page_urls:
                log.info(f"No more results on page {page_num} for '{profile_name}'")
                break

            profile_urls.extend(page_urls)
            log.info(f"  Page {page_num}: {len(page_urls)} URLs (total: {len(profile_urls)})")

            if len(profile_urls) >= profile_max:
                break

            try:
                next_btn = await page.query_selector(
                    "button[aria-label='Next page'], button.mat-paginator-navigation-next"
                )
                if next_btn:
                    is_disabled = await next_btn.is_disabled()
                    if is_disabled:
                        log.info(f"  No more pages for '{profile_name}'")
                        break
                    await next_btn.click()
                    await page.wait_for_timeout(2000)
                    page_num += 1
                else:
                    log.info(f"  No pagination button found")
                    break
            except Exception as e:
                log.warning(f"  Pagination error: {e}")
                break

        log.info(f"Profile '{profile_name}': {len(profile_urls)} URLs found")
        all_urls.extend(profile_urls[:profile_max])

    seen, unique = set(), []
    for url in all_urls:
        if url not in seen:
            seen.add(url)
            unique.append(url)

    log.info(f"Biddingo total unique URLs: {len(unique)}")
    return unique[:max_results]


async def extract_opportunity(page, url: str) -> Optional[dict]:
    """Extract fields from a Biddingo opportunity detail page."""
    IRRELEVANT = [
        "gravel", "asphalt", "paving", "concrete", "watermain", "sewer",
        "culvert", "road", "bridge", "pump", "refrigeration", "food truck",
        "flooring", "furniture", "janitor", "cleaning", "landscaping", "snow",
        "golf", "playground", "railcar", "bus parts", "forklift",
        "breathing apparatus", "sealift", "radios", "stucco", "washroom",
        "renovation materials", "diesel tank", "overhead door", "scrubber",
    ]

    try:
        try:
            await page.goto(url, timeout=30000)
        except Exception as e:
            log.warning(f"Page load timeout: {url}: {e}")
            return None

        await page.wait_for_load_state("domcontentloaded")
        try:
            await page.wait_for_selector(".card", timeout=8000)
        except Exception:
            pass
        await page.wait_for_timeout(1000)

        body_text = await page.inner_text("body")

        for phrase in ["Upgrade to access", "BAD GATEWAY", "502", "503", "404 Not Found"]:
            if phrase.lower() in body_text.lower():
                log.info(f"  Skipped: {phrase}")
                return None

        # FIX Bug 1: extract last long number from URL, not first number
        # URL pattern: /dashboard/bid/1/11003308/1001008/verification
        # We want the last 4+ digit number (the actual bid ID)
        numbers = re.findall(r'/(\d{4,})', url)
        bid_id = numbers[-1] if numbers else url.split("/")[-1]
        log.info(f"  Extracted bid_id: {bid_id} from URL: {url}")

        solicitation_type = ""
        solicitation_num  = ""
        reference_num     = ""
        location          = ""
        closing_date      = ""
        published_date    = ""
        contract_duration = ""
        bid_intent        = ""
        bid_sub_type      = ""
        qa_deadline       = ""
        title             = ""
        organization      = ""
        contact_name      = ""
        contact_email     = ""
        description       = ""

        try:
            labels = await page.query_selector_all("span.font-weight-600, span.text-color-primary")
            for lbl in labels:
                lbl_text   = (await lbl.inner_text()).strip()
                parent_el  = await lbl.evaluate_handle("el => el.parentElement")
                parent_el  = parent_el.as_element()
                if not parent_el:
                    continue
                next_el = await parent_el.evaluate_handle("el => el.nextElementSibling")
                next_el = next_el.as_element()
                if not next_el:
                    continue
                val = (await next_el.inner_text()).strip()
                if lbl_text == "Solicitation Name" and val:
                    title = val
                elif lbl_text == "Solicitation Number" and val:
                    solicitation_num = val
                elif lbl_text == "Published Date" and val:
                    published_date = val
                elif lbl_text == "Closing Date" and val:
                    closing_date = val
                elif lbl_text == "Value Range" and val and val != "Not Applicable":
                    contract_duration = val
        except Exception as e:
            log.warning(f"  Label extraction error: {e}")

        title_lower = title.lower()
        for kw in IRRELEVANT:
            if kw in title_lower:
                log.info(f"  Skipped irrelevant: {title[:50]}")
                return None

        try:
            card = await page.query_selector(".card, [class*='card']")
            if card:
                card_text = await card.inner_text()
                lines = [l.strip() for l in card_text.split("\n") if l.strip() and len(l.strip()) > 2]
                start = 1 if lines and lines[0] == "Solicitation Overview" else 0
                if start < len(lines):
                    organization = lines[start]
                email_match = re.search(r"[\w.+-]+@[\w-]+\.[\w.]+", card_text)
                if email_match:
                    contact_email = email_match.group(0)
                contact_idx = card_text.find("Contact:")
                if contact_idx > -1:
                    after      = card_text[contact_idx+8:contact_idx+100]
                    name_match = re.search(r"([A-Z][a-z]+ [A-Z][a-z]+)", after)
                    if name_match:
                        contact_name = name_match.group(1)
        except Exception as e:
            log.warning(f"  Card extraction error: {e}")

        log.info(f"  Fields: org='{organization}' title='{title}' closing='{closing_date}'")

        days_to_close = 0
        if closing_date:
            for fmt in ["%m/%d/%Y", "%Y-%m-%d", "%B %d, %Y", "%d/%m/%Y",
                        "%Y/%m/%d", "%m/%d/%Y %I:%M:%S %p %Z"]:
                try:
                    close_dt      = datetime.strptime(closing_date.split(" ")[0], fmt)
                    days_to_close = max(0, (close_dt - datetime.now()).days)
                    break
                except Exception:
                    continue

        # FIX Bug 3: description — try CSS selectors first, fall back to body_text
        for sel in [".description", ".tender-description", ".bid-description",
                    ".notice-body", "[class*='description']"]:
            el = await page.query_selector(sel)
            if el:
                description = (await el.inner_text()).strip()[:500]
                if description:
                    break

        if not description and body_text:
            # Fall back: extract a clean snippet from body text
            # Skip header junk by finding title in body and taking text after it
            title_pos = body_text.find(title) if title else -1
            if title_pos > -1:
                snippet = body_text[title_pos + len(title):title_pos + len(title) + 600].strip()
                description = snippet[:500] if snippet else body_text[:500]
            else:
                description = body_text[:500]

        opp = {
            "biddingo_id"        : bid_id,
            # FIX Bug 2: platform_id correctly set so combiner can track this row
            "platform_id"        : f"Biddingo:{bid_id}",
            "url"                : url,
            "title"              : title,
            "organization"       : organization,
            "issuing_org"        : organization,
            "solicitation_number": solicitation_num,
            "solicitation_type"  : solicitation_type,
            "reference_number"   : reference_num,
            "closing_date"       : closing_date,
            "days_to_close"      : days_to_close,
            "published_date"     : published_date,
            "location"           : location,
            "description"        : description,
            "contact_name"       : contact_name,
            "contact_email"      : contact_email,
            "bid_intent"         : bid_intent,
            "bid_submission_type": bid_sub_type,
            "qa_deadline"        : qa_deadline,
            "contract_duration"  : contract_duration,
            "agreement_types"    : [],
            "matched_capabilities": [],
            "matched_signals"    : [],
            "score"              : 0,
            "recommendation"     : "",
        }

        log.info(f"  Extracted: {opp['title'][:60]} | platform_id={opp['platform_id']} ({days_to_close}d left)")
        return opp

    except Exception as e:
        log.error(f"Failed to extract {url}: {e}")
        return None


async def run_biddingo_scan(
    search_profiles: list,
    capabilities: list,
    signals: list,
    known_clients: list,
    min_days: int = 21,
    max_results: int = 50,
) -> list:
    """
    Full Biddingo scan. Returns list of scored opportunity dicts.
    Called by runner.py. Scoring via shared scorer.py.
    """
    from playwright.async_api import async_playwright
    from scorer import score_opportunity_dict

    opportunities = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=os.getenv("HEADLESS", "true").lower() == "true",
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--window-size=1920,1080",
            ]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="America/Toronto",
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        page = await context.new_page()

        try:
            await login(page)
            urls = await search_opportunities(page, search_profiles, max_results)
            log.info(f"Biddingo: {len(urls)} opportunities to process")

            for i, url in enumerate(urls, 1):
                log.info(f"Processing {i}/{len(urls)}: {url}")
                opp = await extract_opportunity(page, url)
                if opp is None:
                    continue
                if 0 < opp["days_to_close"] < min_days:
                    log.info(f"  Skipped — only {opp['days_to_close']} days left")
                    continue
                opp = score_opportunity_dict(opp, capabilities, signals)
                opp["sources"] = ["Biddingo"]
                opportunities.append(opp)

        finally:
            await browser.close()

    log.info(f"Biddingo scan complete: {len(opportunities)} opportunities")
    return opportunities