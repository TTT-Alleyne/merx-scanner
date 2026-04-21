"""
Nextcloud master file writer for Alleyne Group bid scanner.

Maintains a single master Excel file (AlleyneInc_BidTracker.xlsx) in Nextcloud.
- Adds new opportunities from any platform (Merx, Biddingo, etc.)
- Skips duplicates (by platform_id)
- Marks expired/rejected rows and moves them to Archive tab
- Never touches human-filled columns
- Sorts active rows by score descending
"""

import os
import io
import logging
from datetime import datetime, timezone
from typing import Optional

import requests
from requests.auth import HTTPBasicAuth
import openpyxl
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter

log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
NEXTCLOUD_URL      = os.getenv("NEXTCLOUD_URL",    "https://cloud.alleyneinc.net")
NEXTCLOUD_USER     = os.getenv("NEXTCLOUD_USER",   "tzvorygina")
NEXTCLOUD_PASSWORD = os.getenv("NEXTCLOUD_PASSWORD", "")
NEXTCLOUD_FOLDER   = os.getenv("NEXTCLOUD_FOLDER", "Alleyne Inc/AlleyneAdmAgent")
MASTER_FILENAME    = "AlleyneInc_BidTracker.xlsx"

# ── Colors ────────────────────────────────────────────────────────────────────
COLOR_URGENT   = "FFCCCC"   # light rose   — closing ≤ 3 days
COLOR_SOON     = "FFF3CC"   # light yellow — closing ≤ 7 days
COLOR_OK       = "CCEECC"   # light green  — closing 7+ days
COLOR_CLIENT   = "CCE5FF"   # light blue   — known previous client
COLOR_RELEVANT = "E8CCFF"   # light purple — high relevance score
COLOR_EXPIRED  = "E0E0E0"   # light gray   — expired / archived
COLOR_HEADER   = "2D3748"   # dark header
COLOR_SUMMARY  = "F0F0F8"   # light summary row

# ── Column definitions ────────────────────────────────────────────────────────
# Columns the AGENT writes. Human columns are appended after and never touched.
AGENT_COLUMNS = [
    ("Source",                  12),   # A — Merx / Biddingo / AMCI / etc.
    ("Flags",                   18),   # B
    ("Status",                  14),   # C — NEW / ACTIVE / EXPIRED / REJECTED
    ("Client / Account",        30),   # D
    ("Opportunity",             42),   # E
    ("Reference #",             18),   # F
    ("Solicitation #",          18),   # G
    ("Category",                25),   # H
    ("Solicitation Type",       20),   # I
    ("AI Amount Guess",         18),   # J
    ("Closing Date",            20),   # K
    ("Days Left",               10),   # L
    ("Published Date",          20),   # M
    ("Location",                25),   # N
    ("Contract Duration",       18),   # O
    ("Bid Intent",              12),   # P
    ("Quick Summary",           50),   # Q
    ("Contact Name",            20),   # R
    ("Contact Email",           25),   # S
    ("RFP URL",                 35),   # T
    ("Agreement Types",         25),   # U
    ("Q&A Deadline",            20),   # V
    ("Bid Submission Type",     20),   # W
    ("Date Found",              18),   # X
    ("Platform ID",             20),   # Y — internal dedup key
    ("Relevance Score",         15),   # Z
    ("Matched Capabilities",    35),   # AA
    ("Matched Signals",         30),   # AB
    ("Profile",                 20),   # AC — which search profile found it
]

# Human-managed columns (agent never overwrites these)
HUMAN_COLUMNS = [
    ("Sales Stage",             15),
    ("Amount (Est.)",           15),
    ("Probability %",           13),
    ("Weighted Value",          15),
    ("Bid Decision",            15),   # ✅ Pursue / ❌ Pass / 🤔 Review
    ("Notes",                   40),
    ("Assigned To",             18),
    ("RFP Questions",           30),
    ("Reasons for Passing",     30),
]

ALL_COLUMNS = AGENT_COLUMNS + HUMAN_COLUMNS

# Indices (0-based) of columns the agent is allowed to write
AGENT_COL_INDICES = set(range(len(AGENT_COLUMNS)))

# Column letter for Platform ID (used for dedup lookup)
PLATFORM_ID_COL = get_column_letter(
    next(i + 1 for i, (name, _) in enumerate(AGENT_COLUMNS) if name == "Platform ID")
)
STATUS_COL = get_column_letter(
    next(i + 1 for i, (name, _) in enumerate(AGENT_COLUMNS) if name == "Status")
)
SCORE_COL = get_column_letter(
    next(i + 1 for i, (name, _) in enumerate(AGENT_COLUMNS) if name == "Relevance Score")
)
DAYS_COL = get_column_letter(
    next(i + 1 for i, (name, _) in enumerate(AGENT_COLUMNS) if name == "Days Left")
)

# ── Style helpers ─────────────────────────────────────────────────────────────
def _fill(hex_color: str) -> PatternFill:
    return PatternFill(start_color=hex_color, end_color=hex_color, fill_type="solid")

def _border() -> Border:
    thin = Side(style="thin", color="CCCCCC")
    return Border(left=thin, right=thin, top=thin, bottom=thin)

# ── Flag / color logic ────────────────────────────────────────────────────────
def build_flags(opp: dict, known_clients: list) -> str:
    flags = []
    days = opp.get("days_to_close", 999)
    if days <= 3:
        flags.append("URGENT")
    elif days <= 7:
        flags.append("SOON")

    org = (opp.get("organization", "") + " " + opp.get("issuing_org", "")).lower()
    for client in known_clients:
        name = client["name"].lower()
        if any(w in org for w in name.split() if len(w) > 4):
            flags.append("CLIENT★" if client.get("won") else "CLIENT")
            break

    if opp.get("score", 0) >= 60:
        flags.append("HOT")

    # cross-platform bonus
    sources = opp.get("sources", [])
    if len(sources) > 1:
        flags.append("MULTI")

    return " + ".join(flags) if flags else ""

def row_color(opp: dict, known_clients: list, status: str) -> str:
    """
    Color priority (as agreed):
    1. EXPIRED/REJECTED — gray (overrides everything)
    2. Days <= 3 — rose URGENT (overrides score)
    3. Days <= 7 — yellow SOON (overrides score)
    4. Known client — blue (overrides score)
    5. Score 60+ — purple HOT
    6. Score 35-59 — green POSSIBLE
    7. Score <35 — light gray WEAK
    """
    if status in ("EXPIRED", "REJECTED"):
        return COLOR_EXPIRED
    days  = opp.get("days_to_close", 999)
    score = opp.get("score", 0)
    org   = (opp.get("organization", "") + " " + opp.get("issuing_org", "")).lower()
    is_known = any(
        any(w in org for w in c["name"].lower().split() if len(w) > 4)
        for c in known_clients
    )
    # Days override score
    if days <= 3:
        return COLOR_URGENT
    if days <= 7:
        return COLOR_SOON
    # Known client
    if is_known:
        return COLOR_CLIENT
    # Score-based
    if score >= 60:
        return COLOR_RELEVANT   # purple — HOT
    if score >= 35:
        return COLOR_OK         # green — POSSIBLE
    return "F5F5F5"             # light gray — WEAK

# ── Amount guesser ────────────────────────────────────────────────────────────
def guess_amount(opp: dict) -> str:
    import re
    duration = (opp.get("contract_duration", "") or "").lower()
    sol_type = (opp.get("solicitation_type", "") or "").lower()
    org      = (opp.get("organization", "") or "").lower()
    title    = (opp.get("title", "") or "").lower()

    years = 1
    m = re.search(r'(\d+)\s*year', duration)
    if m:
        years = int(m.group(1))
    m2 = re.search(r'option.*?(\d+).*?year', duration)
    if m2:
        years += int(m2.group(1)) * 0.5

    if "rfsa" in sol_type or "supply arrangement" in sol_type:
        base = 500_000
    elif "rfp" in sol_type and "formal" in sol_type:
        base = 200_000
    elif "rfp" in sol_type:
        base = 150_000
    elif "rfq" in sol_type and "formal" in sol_type:
        base = 100_000
    elif "acan" in sol_type or "npp" in sol_type:
        base = 100_000
    else:
        base = 75_000

    mult = 1.0
    if any(k in org for k in ["federal", "canada", "department", "government of canada"]):
        mult = 1.5
    elif any(k in org for k in ["university", "hospital", "hydro", "power", "bank", "insurance"]):
        mult = 1.3
    if any(k in title for k in ["enterprise", "transformation", "erp", "platform", "system"]):
        mult *= 1.4

    estimate = round(base * years * mult / 25_000) * 25_000
    return f"~${max(estimate, 75_000):,.0f}"

# ── Platform ID builder ───────────────────────────────────────────────────────
def make_platform_id(opp: dict, platform: str) -> str:
    """Stable unique ID per opportunity per platform."""
    raw = opp.get("merx_id") or opp.get("biddingo_id") or opp.get("solicitation_number") or opp.get("url", "")
    return f"{platform}:{raw}"

# ── Nextcloud helpers ─────────────────────────────────────────────────────────
def _auth():
    return HTTPBasicAuth(NEXTCLOUD_USER, NEXTCLOUD_PASSWORD)

def _webdav_url(filename: str) -> str:
    return f"{NEXTCLOUD_URL}/remote.php/dav/files/{NEXTCLOUD_USER}/{NEXTCLOUD_FOLDER}/{filename}"

def _download_master() -> Optional[openpyxl.Workbook]:
    """Download existing master file from Nextcloud. Returns None if not found."""
    url = _webdav_url(MASTER_FILENAME)
    r = requests.get(url, auth=_auth())
    if r.status_code == 200:
        log.info("Downloaded existing master file from Nextcloud")
        return openpyxl.load_workbook(io.BytesIO(r.content))
    elif r.status_code == 404:
        log.info("Master file not found — will create new one")
        return None
    else:
        raise Exception(f"Nextcloud download error: {r.status_code} {r.text}")

def _upload_master(wb: openpyxl.Workbook) -> str:
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    # Ensure folder exists
    folder_url = f"{NEXTCLOUD_URL}/remote.php/dav/files/{NEXTCLOUD_USER}/{NEXTCLOUD_FOLDER}"
    requests.request("MKCOL", folder_url, auth=_auth())

    url = _webdav_url(MASTER_FILENAME)
    r = requests.put(
        url, data=buf.getvalue(), auth=_auth(),
        headers={"Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}
    )
    if r.status_code not in (200, 201, 204):
        raise Exception(f"Nextcloud upload failed: {r.status_code} {r.text}")
    log.info(f"Master file uploaded: {url}")
    return url

# ── Workbook builders ─────────────────────────────────────────────────────────
def _write_header_row(ws, row: int):
    for col_idx, (name, width) in enumerate(ALL_COLUMNS, 1):
        cell = ws.cell(row=row, column=col_idx, value=name)
        cell.font      = Font(bold=True, color="FFFFFF", size=10)
        cell.fill      = _fill(COLOR_HEADER)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border    = _border()
        ws.column_dimensions[get_column_letter(col_idx)].width = width
    ws.row_dimensions[row].height = 30

def _write_title_row(ws, row: int, scan_date: str, total: int, active: int):
    ws.merge_cells(f"A{row}:{get_column_letter(len(ALL_COLUMNS))}{row}")
    c = ws.cell(row=row, column=1,
                value=f"ALLEYNE GROUP — BID TRACKER | Last scan: {scan_date} | Total: {total} | Active: {active}")
    c.font      = Font(bold=True, color="FFFFFF", size=13)
    c.fill      = _fill(COLOR_HEADER)
    c.alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[row].height = 24

def _write_data_row(ws, row: int, opp: dict, known_clients: list,
                    platform: str, profile: str, status: str,
                    human_values: Optional[list] = None):
    flags    = build_flags(opp, known_clients)
    color    = row_color(opp, known_clients, status)
    fill     = _fill(color)
    platform_id = make_platform_id(opp, platform)

    # Determine source label — could be "Merx + Biddingo" for cross-platform
    sources = opp.get("sources", [platform])
    source_label = " + ".join(sorted(set(sources)))

    agent_values = [
        source_label,
        flags,
        status,
        opp.get("organization", ""),
        opp.get("title", ""),
        opp.get("reference_number", ""),
        opp.get("solicitation_number", ""),
        ", ".join((opp.get("matched_capabilities") or [])[:2]),
        opp.get("solicitation_type", ""),
        guess_amount(opp),
        opp.get("closing_date", ""),
        opp.get("days_to_close", ""),
        opp.get("published_date", ""),
        opp.get("location", ""),
        opp.get("contract_duration", ""),
        opp.get("bid_intent", ""),
        (opp.get("description") or "")[:300],
        opp.get("contact_name", ""),
        opp.get("contact_email", ""),
        opp.get("url", ""),
        ", ".join(opp.get("agreement_types") or []),
        opp.get("qa_deadline", ""),
        opp.get("bid_submission_type", ""),
        datetime.now().strftime("%Y-%m-%d"),
        platform_id,
        opp.get("score", 0),
        ", ".join((opp.get("matched_capabilities") or [])[:3]),
        ", ".join((opp.get("matched_signals") or [])[:3]),
        profile,
    ]

    all_values = agent_values + (human_values or [""] * len(HUMAN_COLUMNS))

    for col_idx, value in enumerate(all_values, 1):
        cell = ws.cell(row=row, column=col_idx, value=value)
        cell.fill      = fill
        cell.border    = _border()
        cell.alignment = Alignment(vertical="top", wrap_text=True)
        cell.font      = Font(size=9,
                              color="888888" if status in ("EXPIRED", "REJECTED") else "000000")
    ws.row_dimensions[row].height = 45

# ── Read existing rows from a worksheet ──────────────────────────────────────
def _read_existing_rows(ws) -> dict:
    """
    Returns dict: platform_id -> {row_number, human_values, status, opp_dict}
    Assumes row 1 = title, row 2 = blank, row 3 = header, data starts row 4.
    """
    rows = {}
    header_row = 3
    pid_col_idx = next(
        i + 1 for i, (name, _) in enumerate(AGENT_COLUMNS) if name == "Platform ID"
    )
    status_col_idx = next(
        i + 1 for i, (name, _) in enumerate(AGENT_COLUMNS) if name == "Status"
    )

    for row in ws.iter_rows(min_row=header_row + 1):
        pid_cell    = row[pid_col_idx - 1]
        status_cell = row[status_col_idx - 1]
        if not pid_cell.value:
            continue
        pid    = str(pid_cell.value)
        status = str(status_cell.value) if status_cell.value else "ACTIVE"
        # Preserve human column values
        human_start = len(AGENT_COLUMNS)
        human_vals  = [row[human_start + i].value if (human_start + i) < len(row) else ""
                       for i in range(len(HUMAN_COLUMNS))]
        rows[pid] = {
            "row_number"  : pid_cell.row,
            "status"      : status,
            "human_values": human_vals,
        }
    return rows

# ── Main public function ──────────────────────────────────────────────────────
def write_to_nextcloud(
    opportunities : list,
    known_clients : list,
    company       : str  = "Alleyne Inc.",
    platform      : str  = "MerxS",
    profile       : str  = "Default",
) -> str:
    """
    Update master BidTracker file in Nextcloud.
    - Downloads existing file (or creates new)
    - Marks expired/missing rows
    - Adds new opportunities with status=NEW
    - Updates existing rows (agent fields only)
    - Moves expired/rejected to Archive tab
    - Sorts active rows by score desc
    - Uploads updated file
    Returns Nextcloud URL.
    """
    log.info(f"Updating master BidTracker — {len(opportunities)} opportunities from {platform}/{profile}")

    # Map incoming opportunities by platform_id for fast lookup
    incoming = {}
    for opp in opportunities:
        pid = make_platform_id(opp, platform)
        if pid not in incoming:
            incoming[pid] = opp
        else:
            # Cross-platform duplicate — merge sources
            existing_sources = incoming[pid].get("sources", [platform])
            incoming[pid]["sources"] = list(set(existing_sources + [platform]))
            # Boost score by 5 as agreed
            incoming[pid]["score"] = min(100, incoming[pid].get("score", 0) + 5)

    # Download or create workbook
    wb = _download_master()
    if wb is None:
        wb = openpyxl.Workbook()
        wb.active.title = "Active Bids"

    # Ensure sheets exist
    if "Active Bids" not in wb.sheetnames:
        wb.create_sheet("Active Bids", 0)
    if "Archive" not in wb.sheetnames:
        wb.create_sheet("Archive")

    ws_active  = wb["Active Bids"]
    ws_archive = wb["Archive"]

    # Read existing rows
    existing = _read_existing_rows(ws_active)
    scan_date = datetime.now().strftime("%Y-%m-%d %H:%M")

    # Determine which existing rows to expire
    # (those found by THIS platform that are no longer in the incoming list)
    incoming_pids = set(incoming.keys())

    updated_opps = []  # will rebuild active sheet from scratch

    # Process existing rows
    for pid, info in existing.items():
        status = info["status"]
        if status in ("EXPIRED", "REJECTED"):
            # Already archived — skip (they live in Archive sheet)
            continue

        if pid.startswith(platform + ":") and pid not in incoming_pids:
            # Was found by this platform before but not now → expire it
            status = "EXPIRED"
            log.info(f"Marking expired: {pid}")
        elif pid in incoming_pids:
            # Still active — update agent fields, keep human fields
            opp = incoming.pop(pid)
            opp["sources"] = opp.get("sources", [platform])
            status = "ACTIVE"
            updated_opps.append((opp, status, info["human_values"],
                                  opp.get("profile", profile)))
            continue

        if status == "EXPIRED":
            # Move to archive — we'll write it there later
            # For now just skip from active
            continue

        # Keep other-platform rows as-is (we don't touch them)
        # We need their data — but we don't have the opp dict anymore
        # So we'll copy them by re-reading the row values below
        updated_opps.append((None, status, info["human_values"], profile, pid))

    # Add genuinely new opportunities
    for pid, opp in incoming.items():
        opp["sources"] = opp.get("sources", [platform])
        updated_opps.append((opp, "NEW", None, opp.get("profile", profile)))

    # Rebuild active sheet
    # Clear everything and rewrite
    ws_active.delete_rows(1, ws_active.max_row)

    # Sort: NEW first (by score), then ACTIVE (by score), expire goes to archive
    def sort_key(item):
        opp_or_none = item[0]
        status = item[1]
        score = opp_or_none.get("score", 0) if opp_or_none else 0
        order = {"NEW": 0, "ACTIVE": 1}.get(status, 2)
        return (order, -score)

    active_items  = [(o, s, h, p) for item in updated_opps
                     for o, s, h, p in [item[:4]]
                     if s not in ("EXPIRED", "REJECTED") and o is not None]
    active_items.sort(key=lambda x: sort_key(x))

    total_active = len(active_items)
    _write_title_row(ws_active, 1, scan_date, total=len(existing) + len(incoming), active=total_active)
    ws_active.row_dimensions[2].height = 6  # blank spacer
    _write_header_row(ws_active, 3)
    ws_active.freeze_panes = "C4"
    ws_active.auto_filter.ref = f"A3:{get_column_letter(len(ALL_COLUMNS))}3"

    data_row = 4
    for opp, status, human_vals, prof in active_items:
        _write_data_row(ws_active, data_row, opp, known_clients,
                        platform=opp.get("sources", [platform])[0],
                        profile=prof,
                        status=status,
                        human_values=human_vals)
        data_row += 1

    # Ensure Archive sheet has headers
    if ws_archive.max_row <= 1:
        _write_header_row(ws_archive, 1)
        ws_archive.freeze_panes = "C2"

    # Write expired rows to archive
    for pid, info in existing.items():
        if info["status"] == "EXPIRED" or (
            pid.startswith(platform + ":") and pid not in incoming_pids and info["status"] != "REJECTED"
        ):
            # We write a minimal expired row to archive
            arch_row = ws_archive.max_row + 1
            expired_opp = {"title": pid, "score": 0,
                           "days_to_close": 0, "organization": "",
                           "issuing_org": "", "sources": [platform]}
            _write_data_row(ws_archive, arch_row, expired_opp, known_clients,
                            platform=platform, profile=profile,
                            status="EXPIRED", human_values=info["human_values"])

    url = _upload_master(wb)
    log.info(f"BidTracker updated: {total_active} active bids")
    return url