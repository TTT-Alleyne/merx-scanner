"""
scorer.py — Alleyne Group shared scoring module.
Used by: scanner.py (Merx), biddingo_driver.py, bonfire_driver.py
Single source of truth for all opportunity scoring logic.
"""

# ── Tier 1: High-value title keywords (up to 40 points) ──────────────────────
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

# ── Tier 3: Premium org types (up to 15 points) ───────────────────────────────
ORG_KEYWORDS_PREMIUM = [
    "insurance", "assurance", "life insurance", "reinsurance",
    "sun life", "manulife", "canada life", "great-west", "intact",
    "desjardins", "co-operators", "beneva", "industrial alliance",
    "blue cross", "green shield", "empire life",
    "bank", "banque", "financial", "trust", "credit union",
    "caisse", "investment", "capital", "asset management",
    "crown corporation", "edc", "export development",
    "bdc", "business development bank", "farm credit",
    "canada mortgage", "cmhc", "cbc", "via rail",
    "hydro", "power corporation", "energy", "utilities",
    "hospital", "health authority", "university", "college",
]

STOPWORDS = {
    "services", "consulting", "management", "strategy", "development",
    "implementation", "planning", "design", "analysis", "support"
}


def score_opportunity_dict(opp: dict, capabilities: list, signals: list) -> dict:
    """
    Score an opportunity dict 0-100 against criteria.
    Works with dict format used by Biddingo and Bonfire drivers.
    Mutates and returns the dict with score, matched_capabilities,
    matched_signals, and recommendation fields set.

    Capabilities items use 'service' field (from criteria_sets.json).
    Signals items use 'keywords' (comma-separated string) and 'priority' fields.
    """
    title = (opp.get("title") or "").lower()
    org   = ((opp.get("organization") or "") + " " + (opp.get("issuing_org") or "")).lower()
    desc  = (opp.get("description") or "").lower()
    sol_type = (opp.get("solicitation_type") or "").lower()
    commodity = (opp.get("commodity_codes") or "").lower()
    text  = f"{title} {org} {desc} {sol_type} {commodity}"

    score = 0
    matched_caps = []
    matched_sigs = []

    # ── Tier 1: Title keyword match (up to 40 points) ─────────────────────────
    tier1 = 0
    for kw in TITLE_KEYWORDS_HIGH:
        if kw in title:
            tier1 += 20
            if tier1 >= 40:
                break
    score += min(tier1, 40)

    # ── Tier 2: Capability matching via service field (up to 40 points) ───────
    cap_score = 0
    for cap in capabilities:
        service = (cap.get("service") or "").lower()
        words   = [w for w in service.split() if len(w) > 4 and w not in STOPWORDS]
        if not words:
            continue
        hits      = sum(1 for w in words if w in text)
        threshold = 1 if len(words) <= 1 else 2
        if hits >= threshold:
            matched_caps.append(cap["service"])
            cap_score += 8
    score += min(cap_score, 40)

    # ── Tier 3: Org type bonus (up to 15 points) ──────────────────────────────
    for org_kw in ORG_KEYWORDS_PREMIUM:
        if org_kw in org:
            score += 15
            break

    # ── Signal matching (up to 20 points) ─────────────────────────────────────
    for sig in signals:
        keywords = (sig.get("keywords") or "").lower().split(",")
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
    days = opp.get("days_to_close", 0) or 0
    if days >= 60:
        score += 5
    elif days >= 30:
        score += 3

    opp["score"]                = min(score, 100)
    opp["matched_capabilities"] = list(dict.fromkeys(matched_caps))[:5]
    opp["matched_signals"]      = list(dict.fromkeys(matched_sigs))[:5]
    opp["recommendation"]       = (
        "STRONG FIT — pursue"   if score >= 60 else
        "POSSIBLE FIT — review" if score >= 35 else
        "WEAK FIT — skip"
    )
    return opp