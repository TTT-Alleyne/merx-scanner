"""
Merx Scanner Agent — Alleyne Group
Logs into Merx, searches for opportunities, scores them against criteria,
and saves results to a JSON report.
"""

import os
import json
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
MERX_LOGIN_URL  = "https://www.merx.com/English/MERX_Solicitations/login.cfm"
MERX_SEARCH_URL = "https://www.merx.com/private/supplier/solicitations/search"
CRITERIA_FILE   = os.getenv("CRITERIA_FILE", "/data/criteria_sets.json")
RESULTS_DIR     = os.getenv("RESULTS_DIR", "/results")
MIN_DAYS_TO_BID = int(os.getenv("MIN_DAYS_TO_BID", "21"))
MAX_RESULTS     = int(os.getenv("MAX_RESULTS", "50"))
HEADLESS        = os.getenv("HEADLESS", "true").lower() == "true"

# ── Data model ────────────────────────────────────────────────────────────────
@dataclass
class Opportunity:
    merx_id: str
    url: str
    title: str
    organization: str
    issuing_org: str
    solicitation_number: str
    solicitation_type: str
    reference_number: str
    source_id: str
    agreement_types: list
    closing_date: str
    days_to_close: int
    published_date: str
    location: str
    description: str
    documents_count: int
    contact_name: str = ""
    contact_email: str = ""
    bid_intent: str = ""
    bid_submission_type: str = ""
    qa_deadline: str = ""
    contract_duration: str = ""
    score: int = 0
    matched_capabilities: list = None
    matched_signals: list = None
    recommendation: str = ""

    def __post_init__(self):
        if self.matched_capabilities is None:
            self.matched_capabilities = []
        if self.matched_signals is None:
            self.matched_signals = []

# ── Criteria loader ───────────────────────────────────────────────────────────
def load_criteria(company: str = "Alleyne Inc."):
    """Load active capabilities, sales signals and search profiles for a company."""
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
    log.info(f"Loaded {len(capabilities)} capabilities, {len(signals)} signals, {len(search_profiles)} search profiles for {company}")
    return capabilities, signals, search_profiles

def build_keyword_query(signals: list) -> str:
    """Build Merx keyword query from sales signals."""
    high    = [s for s in signals if s.get("priority") == "HIGH"]
    medium  = [s for s in signals if s.get("priority") == "MEDIUM"]
    use     = high if high else medium
    terms   = []
    for s in use:
        for kw in s.get("keywords", "").split(","):
            kw = kw.strip().strip('"')
            if kw and len(kw) > 3:
                terms.append(f'"{kw}"')
    unique = list(dict.fromkeys(terms))[:20]
    return " OR ".join(unique)

# ── Scorer ────────────────────────────────────────────────────────────────────

# Tier 1 — High-value title keywords that immediately signal relevance
TITLE_KEYWORDS_HIGH = [
    "management consulting", "it consulting", "digital transformation",
    "knowledge management", "information management", "enterprise architecture",
    "change management", "business transformation", "strategic planning",
    "it strategy", "data analytics", "ai consulting", "cloud migration",
    "business process", "organizational design", "project management",
    "cybersecurity", "privacy", "sharepoint", "erp", "crm",
    "business analyst", "business consultant", "advisory services",
    "professional services", "it advisory", "it services",
]

# Tier 3 — High-value organization types (insurance, finance, crown corps)
ORG_KEYWORDS_PREMIUM = [
    # Insurance
    "insurance", "assurance", "life insurance", "reinsurance",
    "sun life", "manulife", "canada life", "great-west", "intact",
    "desjardins", "co-operators", "beneva", "industrial alliance",
    "blue cross", "green shield", "empire life",
    # Banks & financial
    "bank", "banque", "financial", "trust", "credit union",
    "caisse", "investment", "capital", "asset management",
    # Crown corporations & large federal
    "crown corporation", "edc", "export development",
    "bdc", "business development bank", "farm credit",
    "canada mortgage", "cmhc", "cbc", "via rail",
    "hydro", "power corporation", "energy", "utilities",
    # Large orgs
    "hospital", "health authority", "university", "college",
]

def score_opportunity(opp: Opportunity, capabilities: list, signals: list) -> Opportunity:
    """Score an opportunity 0-100 against criteria using 3-tier system."""
    score = 0
    title = opp.title.lower()
    org   = (opp.organization + " " + opp.issuing_org).lower()
    desc  = opp.description.lower() if opp.description else ""
    text  = f"{title} {org} {desc} {opp.solicitation_type}".lower()

    matched_caps, matched_sigs = [], []

    # ── Tier 1: Title keyword match (up to 40 points) ─────────────────────────
    tier1_score = 0
    for kw in TITLE_KEYWORDS_HIGH:
        if kw in title:
            tier1_score += 20
            if tier1_score >= 40:
                break
    score += min(tier1_score, 40)

    # ── Tier 2: Capability matching from our list (up to 40 points) ───────────
    stopwords = {"services", "consulting", "management", "strategy", "development",
                "implementation", "planning", "design", "analysis", "support"}
    cap_score = 0
    for cap in capabilities:
        service = cap.get("service", "").lower()
        words = [w for w in service.split() if len(w) > 4 and w not in stopwords]
        if not words:
            continue
        hits = sum(1 for w in words if w in text)
        threshold = 1 if len(words) <= 1 else 2
        if hits >= threshold:
            matched_caps.append(cap["service"])
            cap_score += 8
    score += min(cap_score, 40)

    # ── Tier 3: Organization type bonus (up to 15 points) ─────────────────────
    tier3_score = 0
    for org_kw in ORG_KEYWORDS_PREMIUM:
        if org_kw in org:
            tier3_score = 15
            break
    score += tier3_score

    # ── Signal matching (up to 20 points) ─────────────────────────────────────
    for sig in signals:
        keywords = sig.get("keywords", "").lower().split(",")
        for kw in keywords:
            kw = kw.strip().strip('"').strip()
            if len(kw) > 5 and kw in text:
                sig_name = sig.get("signal", "")
                if sig_name not in matched_sigs:
                    matched_sigs.append(sig_name)
                    priority = sig.get("priority", "LOW")
                    score   += {"HIGH": 10, "MEDIUM": 7, "LOW": 3}.get(priority, 3)
                break

    # ── Deadline bonus (up to 5 points) ───────────────────────────────────────
    if opp.days_to_close >= 60:
        score += 5
    elif opp.days_to_close >= 30:
        score += 3

    opp.score                = min(score, 100)
    opp.matched_capabilities = list(dict.fromkeys(matched_caps))[:5]
    opp.matched_signals      = list(dict.fromkeys(matched_sigs))[:5]

    if opp.score >= 60:
        opp.recommendation = "STRONG FIT — pursue"
    elif opp.score >= 30:
        opp.recommendation = "POSSIBLE FIT — review"
    else:
        opp.recommendation = "WEAK FIT — skip"

    return opp

# ── Browser helpers ───────────────────────────────────────────────────────────
def make_driver() -> webdriver.Chrome:
    opts = Options()
    if HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(30)
    return driver

def wait_for(driver, by, selector, timeout=15):
    return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((by, selector)))

def safe_text(driver, by, selector, default="") -> str:
    try:
        return driver.find_element(by, selector).text.strip()
    except NoSuchElementException:
        return default

# ── Login ─────────────────────────────────────────────────────────────────────
def login(driver):
    email    = os.getenv("MERX_EMAIL")
    password = os.getenv("MERX_PASSWORD")
    if not email or not password:
        raise ValueError("MERX_EMAIL and MERX_PASSWORD must be set in .env file")

    log.info("Logging into Merx...")

    # Start from homepage and handle cookies
    driver.get("https://www.merx.com")
    time.sleep(4)
    log.info(f"Homepage loaded: {driver.current_url}")
    driver.save_screenshot("/results/step1_homepage.png")

    # Accept cookies if banner appears
    try:
        cookie_btn = WebDriverWait(driver, 8).until(
            EC.element_to_be_clickable((By.XPATH,
                "//button[contains(text(),'Allow all') or contains(text(),'Accept') or contains(text(),'allow all')]"))
        )
        cookie_btn.click()
        log.info("Cookies accepted")
        time.sleep(2)
    except:
        log.info("No cookie banner found")

    # Click Login link
    try:
        login_link = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH,
                "//a[contains(text(),'Login') or contains(text(),'login') or contains(text(),'Sign in')]"))
        )
        login_link.click()
        log.info("Clicked Login link")
        time.sleep(5)
    except Exception as e:
        log.warning(f"Could not click login link: {e}")
        # Try direct private URL which should trigger SSO redirect
        driver.get("https://www.merx.com/private/supplier/solicitations/search")
        time.sleep(5)

    log.info(f"After login click: {driver.current_url}")
    driver.save_screenshot("/results/step2_after_login_click.png")

    # Save screenshot to see what we got
    driver.save_screenshot("/results/login_page.png")
    log.info(f"Screenshot saved. Current URL: {driver.current_url}")
    log.info(f"Page title: {driver.title}")

    try:
        # Wait for ANY input field to appear
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input"))
        )

        # Try all possible username selectors
        username_field = None
        for sel in ["input[name='username']", "input[type='text']",
                    "input[id='username']", "input[autocomplete='username']"]:
            try:
                username_field = driver.find_element(By.CSS_SELECTOR, sel)
                if username_field.is_displayed():
                    log.info(f"Found username field: {sel}")
                    break
            except:
                continue

        if not username_field:
            raise Exception("Username field not found")

        username_field.clear()
        username_field.send_keys(email)
        time.sleep(1)

        pwd_field = driver.find_element(By.CSS_SELECTOR, "input[type='password']")
        pwd_field.clear()
        pwd_field.send_keys(password)
        time.sleep(1)

        pwd_field.send_keys(Keys.RETURN)
        time.sleep(5)

        log.info(f"Login submitted. URL: {driver.current_url}")
        driver.save_screenshot("/results/after_login.png")

    except Exception as e:
        driver.save_screenshot("/results/login_debug.png")
        log.error(f"Login failed: {e}")
        raise

# ── Search ────────────────────────────────────────────────────────────────────
def is_logged_in(driver) -> bool:
    """Check if we're still logged into Merx."""
    return "idp.merx.com" not in driver.current_url and "merx.com/login" not in driver.current_url

def ensure_logged_in(driver):
    """Re-login if session expired."""
    driver.get(MERX_SEARCH_URL)
    time.sleep(3)
    if not is_logged_in(driver):
        log.info("Session expired — re-logging in...")
        login(driver)
        driver.get(MERX_SEARCH_URL)
        time.sleep(3)

def search_opportunities(driver, search_profiles: list) -> list:
    """Run each search profile and collect unique opportunity URLs."""
    all_urls = []

    for profile in search_profiles:
        profile_name = profile.get("profile_name", "Unknown")
        keywords = profile.get("include_keywords", "")
        max_results = profile.get("max_results", 50)

        if not keywords:
            log.warning(f"Profile '{profile_name}' has no keywords — skipping")
            continue

        log.info(f"Running search profile: {profile_name}")

        # Make sure we're still logged in
        ensure_logged_in(driver)

        # Navigate to search page
        driver.get(MERX_SEARCH_URL)
        time.sleep(4)

        # Enter keywords — Merx search box is in the header
        try:
            # Use JavaScript to find and fill the search box
            # This is more reliable than CSS selectors for dynamic pages
            js_result = driver.execute_script("""
                var inputs = document.querySelectorAll('input');
                var searchBox = null;
                for (var i = 0; i < inputs.length; i++) {
                    var ph = inputs[i].placeholder || '';
                    if (ph.toLowerCase().includes('keyword') || 
                        ph.toLowerCase().includes('search') ||
                        inputs[i].type === 'search') {
                        searchBox = inputs[i];
                        break;
                    }
                }
                if (searchBox) {
                    searchBox.focus();
                    searchBox.value = arguments[0];
                    searchBox.dispatchEvent(new Event('input', {bubbles: true}));
                    searchBox.dispatchEvent(new Event('change', {bubbles: true}));
                    return 'found:' + searchBox.placeholder;
                }
                return 'not_found';
            """, keywords[:500])

            log.info(f"JS search result: {js_result}")

            if js_result and js_result.startswith('found'):
                # Now click the search button or press Enter
                time.sleep(1)
                try:
                    search_btn = driver.find_element(By.CSS_SELECTOR,
                        "button[type='submit'], input[type='submit'], .search-btn, button.search")
                    search_btn.click()
                except:
                    # Try pressing Enter on the search box
                    from selenium.webdriver.common.keys import Keys as K
                    inputs = driver.find_elements(By.CSS_SELECTOR, "input")
                    for inp in inputs:
                        ph = inp.get_attribute("placeholder") or ""
                        if "keyword" in ph.lower() or "search" in ph.lower():
                            inp.send_keys(K.RETURN)
                            break
                time.sleep(5)
                log.info(f"Search submitted. URL: {driver.current_url}")
                driver.save_screenshot(f"/results/search_{profile_name.replace(' ', '_')}.png")
            else:
                log.warning(f"Search box not found via JS — trying direct URL approach")
                # Build search URL directly
                import urllib.parse
                encoded = urllib.parse.quote(keywords[:500])
                search_url = f"{MERX_SEARCH_URL}?keywords={encoded}"
                driver.get(search_url)
                time.sleep(5)
                log.info(f"Direct URL search. URL: {driver.current_url}")
                driver.save_screenshot(f"/results/search_{profile_name.replace(' ', '_')}.png")

        except Exception as e:
            log.warning(f"Search failed for '{profile_name}': {e}")
            driver.save_screenshot(f"/results/search_error_{profile_name.replace(' ', '_')}.png")
            continue

        # Collect URLs from this search
        profile_urls = []
        page = 1

        while len(profile_urls) < max_results:
            # Only look for actual opportunity links
            # Real Merx opportunities have these patterns:
            # /private/supplier/interception/open-solicitation/NUMBER
            # /private/supplier/interception/view-notice/NUMBER
            all_links = driver.find_elements(By.CSS_SELECTOR, "a[href]")
            page_urls = []
            for link in all_links:
                href = link.get_attribute("href") or ""
                # Must contain interception AND either open-solicitation or view-notice
                if "interception/open-solicitation/" in href or "interception/view-notice/" in href:
                    if href not in all_urls and href not in profile_urls:
                        page_urls.append(href)

            if not page_urls:
                log.info(f"No more results on page {page} for '{profile_name}'")
                break

            profile_urls.extend(page_urls)
            log.info(f"  Page {page}: {len(page_urls)} URLs (profile total: {len(profile_urls)})")

            # Next page
            try:
                next_btn = driver.find_element(By.CSS_SELECTOR,
                    "a[aria-label='Next'], a.next, button.next, a[rel='next'], .pagination .next a")
                if "disabled" in (next_btn.get_attribute("class") or ""):
                    break
                next_btn.click()
                time.sleep(3)
                page += 1
            except NoSuchElementException:
                break

        log.info(f"Profile '{profile_name}': found {len(profile_urls)} URLs")
        all_urls.extend(profile_urls[:max_results])

    # Deduplicate
    seen = set()
    unique_urls = []
    for url in all_urls:
        if url not in seen:
            seen.add(url)
            unique_urls.append(url)

    log.info(f"Total unique URLs across all profiles: {len(unique_urls)}")
    return unique_urls[:MAX_RESULTS]

# ── Extract opportunity detail ────────────────────────────────────────────────
def extract_opportunity(driver, url: str) -> Optional[Opportunity]:
    """Extract all fields from an opportunity detail page."""
    import re
    try:
        try:
            driver.get(url)
        except Exception as e:
            log.warning(f"Page load timeout for {url}: {e}")
            return None
        time.sleep(2)

        # Check for upgrade wall or errors
        page_text = driver.find_element(By.TAG_NAME, "body").text
        skip_phrases = [
            "Upgrade to access more",
            "upgrade your account",
            "BAD GATEWAY",
            "502",
            "503",
            "404",
            "Error 404",
        ]
        for phrase in skip_phrases:
            if phrase.lower() in page_text.lower():
                log.info(f"  Skipped — {phrase}")
                return None

        # Extract merx_id from URL
        merx_id = re.search(r'(\d{8,})', url)
        merx_id = merx_id.group(1) if merx_id else url.split("/")[-1]

        # ── Title ──────────────────────────────────────────────────────────────
        title = ""
        for sel in ["h1.solicitation-title", "h1", ".notice-title", ".solicitation-header h1"]:
            try:
                el = driver.find_element(By.CSS_SELECTOR, sel)
                title = el.text.strip()
                if title and len(title) > 5:
                    break
            except:
                pass

        # Skip irrelevant opportunities based on title keywords
        irrelevant_keywords = [
            "gravel", "asphalt", "paving", "concrete", "watermain", "sewer",
            "culvert", "road", "bridge", "pump", "boerger", "refrigeration",
            "food truck", "flooring", "furniture", "janitor", "cleaning",
            "landscaping", "snow", "golf", "food beverage", "playground",
            "railcar", "bus parts", "pickup truck", "forklift", "DAS",
            "breathing apparatus", "SCBA", "sealift", "radios", "accessories",
            "stucco", "lock upgrade", "washroom", "renovation materials",
            "convault", "diesel tank", "overhead door", "scrubber",
        ]
        title_lower = title.lower()
        for kw in irrelevant_keywords:
            if kw.lower() in title_lower:
                log.info(f"  Skipped irrelevant: {title[:50]}")
                return None

        # ── Page source for field extraction ───────────────────────────────────
        # Get all text content organized by labels
        def get_field_value(label: str) -> str:
            """Find value next to a label on the page."""
            try:
                # Try finding by xpath — look for label text then get sibling/next element
                els = driver.find_elements(By.XPATH,
                    f"//*[contains(text(),'{label}')]/../following-sibling::*[1] | "
                    f"//*[contains(text(),'{label}')]/following-sibling::*[1]"
                )
                for el in els:
                    text = el.text.strip()
                    if text and text != label:
                        return text
            except:
                pass
            return ""

        # ── Organization ───────────────────────────────────────────────────────
        organization = ""
        for label in ["Owner Organization", "Issuing Organization", "Organization"]:
            val = get_field_value(label)
            if val:
                organization = val
                break

        # Fallback — look for org in page structure
        if not organization:
            try:
                org_el = driver.find_element(By.CSS_SELECTOR,
                    ".organization-name, .issuing-org, [class*='organization']")
                organization = org_el.text.strip()
            except:
                pass

        # ── Solicitation details ───────────────────────────────────────────────
        solicitation_type   = get_field_value("Solicitation Type")
        solicitation_number = get_field_value("Solicitation Number")
        reference_number    = get_field_value("Reference Number")
        source_id           = get_field_value("Source ID")
        location            = get_field_value("Location")
        contract_duration   = get_field_value("Purchase Type") or get_field_value("Duration")
        bid_intent          = get_field_value("Bid Intent")
        bid_submission_type = get_field_value("Bid Submission Type")
        qa_deadline         = get_field_value("Question Acceptance Deadline")

        # ── Dates ──────────────────────────────────────────────────────────────
        closing_date    = get_field_value("Closing Date")
        published_date  = get_field_value("Publication") or get_field_value("Published")

        # Days to close — try countdown timer first
        days_to_close = 0
        try:
            # Look for "Xd Yh Zm" pattern in page
            countdown_text = ""
            for sel in ["[class*='countdown']", "[class*='time-left']", "[class*='bid-timer']", "[class*='timer']"]:
                try:
                    el = driver.find_element(By.CSS_SELECTOR, sel)
                    countdown_text = el.text
                    break
                except:
                    pass

            if countdown_text:
                d_match = re.search(r'(\d+)\s*d', countdown_text)
                if d_match:
                    days_to_close = int(d_match.group(1))
            elif closing_date:
                # Parse from closing date string
                from datetime import datetime, timezone
                date_patterns = [
                    "%Y/%m/%d %I:%M:%S %p %Z",
                    "%Y/%m/%d %I:%M:%S %p",
                    "%Y-%m-%d",
                ]
                for pattern in date_patterns:
                    try:
                        close_dt = datetime.strptime(closing_date.replace(" EDT", "").replace(" EST", "").strip(), pattern)
                        days_to_close = max(0, (close_dt - datetime.now()).days)
                        break
                    except:
                        continue
        except:
            pass

        # ── Contact info ───────────────────────────────────────────────────────
        contact_name  = ""
        contact_email = ""
        try:
            contact_section_text = ""
            for sel in ["[class*='contact']", "#contact", ".contact-info"]:
                try:
                    el = driver.find_element(By.CSS_SELECTOR, sel)
                    contact_section_text = el.text
                    break
                except:
                    pass

            if not contact_section_text:
                # Try finding "Contact Information" header and getting next element
                try:
                    headers = driver.find_elements(By.XPATH,
                        "//*[contains(text(),'Contact Information')]")
                    if headers:
                        parent = headers[0].find_element(By.XPATH, "..")
                        contact_section_text = parent.text
                except:
                    pass

            # Extract email from contact section
            email_match = re.search(r'[\w.+-]+@[\w-]+\.[\w.]+', contact_section_text)
            if email_match:
                contact_email = email_match.group(0)

            # Extract name — first line that's not an email
            lines = [l.strip() for l in contact_section_text.split("\n") if l.strip()]
            for line in lines:
                if "@" not in line and len(line) > 3 and "Contact" not in line:
                    contact_name = line
                    break
        except:
            pass

        # ── Description ────────────────────────────────────────────────────────
        description = ""
        try:
            for sel in [".notice-body", ".description", "[class*='notice-content']",
                       "[class*='description']", ".solicitation-description"]:
                try:
                    el = driver.find_element(By.CSS_SELECTOR, sel)
                    description = el.text.strip()[:500]
                    if description:
                        break
                except:
                    pass

            if not description:
                description = get_field_value("Description")[:500] if get_field_value("Description") else ""
        except:
            pass

        # ── Agreement types ────────────────────────────────────────────────────
        agreement_types = []
        try:
            agreement_text = get_field_value("Agreement Types") or get_field_value("Agreement Type")
            if agreement_text:
                agreement_types = [a.strip() for a in agreement_text.split(",") if a.strip()]
        except:
            pass

        # ── Documents ──────────────────────────────────────────────────────────
        docs_count = 0
        try:
            docs_tab = driver.find_element(By.XPATH,
                "//*[contains(text(),'Documents') or contains(text(),'documents')]")
            nums = re.findall(r'\d+', docs_tab.text)
            if nums:
                docs_count = int(nums[0])
        except:
            pass

        opp = Opportunity(
            merx_id             = merx_id,
            url                 = url,
            title               = title,
            organization        = organization,
            issuing_org         = get_field_value("Issuing Organization"),
            solicitation_number = solicitation_number,
            solicitation_type   = solicitation_type,
            reference_number    = reference_number,
            source_id           = source_id,
            agreement_types     = agreement_types,
            closing_date        = closing_date,
            days_to_close       = days_to_close,
            published_date      = published_date,
            location            = location,
            description         = description,
            documents_count     = docs_count,
            contact_name        = contact_name,
            contact_email       = contact_email,
            bid_intent          = bid_intent,
            bid_submission_type = bid_submission_type,
            qa_deadline         = qa_deadline,
            contract_duration   = contract_duration,
        )

        log.info(f"  Extracted: {opp.title[:60]} ({opp.days_to_close}d left)")
        return opp

    except Exception as e:
        log.error(f"Failed to extract {url}: {e}")
        return None

# ── Report writer ─────────────────────────────────────────────────────────────
def save_report(opportunities: list, company: str):
    Path(RESULTS_DIR).mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename  = f"{RESULTS_DIR}/merx_scan_{company.replace(' ', '_')}_{timestamp}.json"

    report = {
        "scan_date"     : datetime.now(timezone.utc).isoformat(),
        "company"       : company,
        "min_days_to_bid": MIN_DAYS_TO_BID,
        "total_found"   : len(opportunities),
        "strong_fit"    : sum(1 for o in opportunities if o.score >= 70),
        "possible_fit"  : sum(1 for o in opportunities if 40 <= o.score < 70),
        "weak_fit"      : sum(1 for o in opportunities if o.score < 40),
        "opportunities" : [asdict(o) for o in sorted(opportunities, key=lambda x: x.score, reverse=True)]
    }

    with open(filename, "w") as f:
        json.dump(report, f, indent=2)
    log.info(f"Report saved: {filename}")

    # Also save a human-readable summary
    summary_file = f"{RESULTS_DIR}/merx_summary_{timestamp}.txt"
    with open(summary_file, "w") as f:
        f.write(f"MERX SCAN REPORT — {company}\n")
        f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write(f"{'='*60}\n\n")
        f.write(f"TOTAL FOUND: {report['total_found']}\n")
        f.write(f"  STRONG FIT:   {report['strong_fit']}\n")
        f.write(f"  POSSIBLE FIT: {report['possible_fit']}\n")
        f.write(f"  WEAK FIT:     {report['weak_fit']}\n\n")
        f.write(f"{'='*60}\n")
        f.write("TOP OPPORTUNITIES\n")
        f.write(f"{'='*60}\n\n")
        for o in [x for x in report['opportunities'] if x['score'] >= 40]:
            f.write(f"[{o['score']:3d}] {o['recommendation']}\n")
            f.write(f"      {o['title']}\n")
            f.write(f"      {o['organization']}\n")
            f.write(f"      Closes: {o['closing_date']} ({o['days_to_close']} days)\n")
            f.write(f"      URL: {o['url']}\n")
            if o['matched_capabilities']:
                f.write(f"      Matches: {', '.join(o['matched_capabilities'][:3])}\n")
            f.write("\n")
    log.info(f"Summary saved: {summary_file}")
    return filename, summary_file

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    from nextcloud_writer import write_to_nextcloud
    import json

    company = os.getenv("COMPANY", "Alleyne Inc.")
    log.info(f"Starting Merx Scanner for {company}")

    capabilities, signals, search_profiles = load_criteria(company)

    if not search_profiles:
        log.warning("No search profiles found — using default keyword query from signals")
        keyword_query = build_keyword_query(signals)
        search_profiles = [{"profile_name": "Default", "include_keywords": keyword_query, "max_results": MAX_RESULTS, "active": True}]

    # Load known clients
    known_clients = []
    try:
        with open("/app/known_clients.json") as f:
            known_clients = json.load(f)["known_clients"]
        log.info(f"Loaded {len(known_clients)} known clients")
    except Exception as e:
        log.warning(f"Could not load known clients: {e}")

    driver = make_driver()
    try:
        login(driver)

        urls = search_opportunities(driver, search_profiles)
        log.info(f"Found {len(urls)} opportunities to process")

        # Process each profile separately
        all_opportunities = []
        for profile in search_profiles:
            profile_name = profile.get("profile_name", "Unknown")
            profile_urls = [u for u in urls if True]  # all urls for now

        # Score all opportunities
        opportunities = []
        for i, url in enumerate(urls, 1):
            log.info(f"Processing {i}/{len(urls)}: {url}")
            opp = extract_opportunity(driver, url)
            if opp is None:
                continue
            if opp.days_to_close < MIN_DAYS_TO_BID and opp.days_to_close > 0:
                log.info(f"  Skipped — only {opp.days_to_close} days left")
                continue
            opp = score_opportunity(opp, capabilities, signals)
            opportunities.append(opp)
            time.sleep(1)

        # Save local backup
        json_file, txt_file = save_report(opportunities, company)

        # Upload to Nextcloud — one file with all results for now
        opp_dicts = [asdict(o) for o in opportunities]
        platform = os.getenv("PLATFORM", "MerxS")
        nextcloud_url = write_to_nextcloud(
            opp_dicts, known_clients,
            company=company,
            profile_name="All-Profiles",
            platform=platform
        )
        log.info(f"Results available at: {nextcloud_url}")

        # Send digest to Joel
        send_digest(opportunities, nextcloud_url)

        log.info(f"Scan complete! {len(opportunities)} opportunities processed")

    finally:
        driver.quit()

def send_digest(opportunities: list, nextcloud_url: str):
    """Send daily digest email to Joel."""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    smtp_host = os.getenv("SMTP_HOST", "mail.alleyneinc.net")
    smtp_user = os.getenv("SMTP_USER", "tzvorygina@alleyneinc.net")
    smtp_pass = os.getenv("SMTP_PASSWORD", "")
    digest_to = os.getenv("DIGEST_TO", "joel.alleyne1@gmail.com")

    if not smtp_pass:
        log.warning("SMTP_PASSWORD not set — skipping digest email")
        return

    strong   = [o for o in opportunities if o.score >= 70]
    possible = [o for o in opportunities if 40 <= o.score < 70]
    scan_date = datetime.now().strftime("%Y-%m-%d %H:%M")

    body = f"""
MERX DAILY SCAN — ALLEYNE INC
Scan date: {scan_date}

SUMMARY
Total opportunities processed: {len(opportunities)}
Strong fits (score 70+):        {len(strong)}
Possible fits (score 40-69):    {len(possible)}

View full sheet in Nextcloud:
{nextcloud_url}

TOP OPPORTUNITIES
{'='*50}
"""
    for opp in sorted(opportunities, key=lambda x: x.score, reverse=True)[:10]:
        body += f"""
[{opp.score}/100] {opp.recommendation}
  {opp.title}
  {opp.organization}
  Closes: {opp.closing_date} ({opp.days_to_close} days left)
  {opp.url}
"""

    msg = MIMEMultipart()
    msg["From"]    = smtp_user
    msg["To"]      = digest_to
    msg["Subject"] = f"Merx Daily Scan — {len(strong)} strong fits — {scan_date}"
    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP_SSL(smtp_host, 465) as server:
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        log.info(f"Digest email sent to {digest_to}")
    except Exception as e:
        log.error(f"Failed to send digest: {e}")

if __name__ == "__main__":
    main()