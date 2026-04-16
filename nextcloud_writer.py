"""
Nextcloud writer for Merx Scanner results.
Uploads Excel spreadsheet directly to Nextcloud via WebDAV.
"""

import os
import io
import logging
from datetime import datetime
import requests
from requests.auth import HTTPBasicAuth
import openpyxl
from openpyxl.styles import (
    PatternFill, Font, Alignment, Border, Side
)
from openpyxl.utils import get_column_letter

log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
NEXTCLOUD_URL      = os.getenv("NEXTCLOUD_URL", "https://cloud.alleyneinc.net")
NEXTCLOUD_USER     = os.getenv("NEXTCLOUD_USER", "tzvorygina")
NEXTCLOUD_PASSWORD = os.getenv("NEXTCLOUD_PASSWORD", "")
NEXTCLOUD_FOLDER   = os.getenv("NEXTCLOUD_FOLDER", "Alleyne Inc/AlleyneAdmAgent")
SHEET_FILENAME     = "Merx_Opportunities.xlsx"

# ── Colors (light pastels as agreed) ─────────────────────────────────────────
COLOR_URGENT   = "FFCCCC"  # light rose   — closing in 3 days
COLOR_SOON     = "FFF3CC"  # light yellow — closing in 7 days
COLOR_OK       = "CCEECC"  # light green  — closing in 7+ days
COLOR_CLIENT   = "CCE5FF"  # light blue   — known client
COLOR_RELEVANT = "E8CCFF"  # light purple — high relevance
COLOR_HEADER   = "2D3748"  # dark header
COLOR_SUMMARY  = "F0F0F8"  # light gray summary rows
COLOR_WHITE    = "FFFFFF"

# ── Column definitions ────────────────────────────────────────────────────────
COLUMNS = [
    ("Flags",                    15),
    ("Client / Account",         30),
    ("Opportunity (Project)",    40),
    ("Reference Number",         18),
    ("Solicitation Number",      18),
    ("Category",                 25),
    ("Solicitation Type",        20),
    ("Sales Stage",              15),
    ("Amount (Est.)",            15),
    ("AI Amount Guess",          18),
    ("Probability %",            13),
    ("Weighted Value",           15),
    ("Closing Date",             20),
    ("Days Left",                10),
    ("Published Date",           20),
    ("Geographic Location",      25),
    ("Contract Duration",        18),
    ("Bid Intent",               12),
    ("Quick Summary",            50),
    ("Requirements of Proposal", 40),
    ("Contact Name",             20),
    ("Contact Email",            25),
    ("RFP Website",              35),
    ("Linked Documents",         35),
    ("Organization Website",     30),
    ("Agreement Types",          25),
    ("Q&A Deadline",             20),
    ("Bid Submission Type",      20),
    ("RFP Questions",            30),
    ("Date Found",               18),
    ("Reasons for Passing",      30),
    ("Merx ID",                  15),
    ("Relevance Score",          15),
    ("Matched Capabilities",     35),
    ("Matched Signals",          30),
]

def make_fill(hex_color: str) -> PatternFill:
    return PatternFill(start_color=hex_color, end_color=hex_color, fill_type="solid")

def make_border() -> Border:
    thin = Side(style="thin", color="CCCCCC")
    return Border(left=thin, right=thin, top=thin, bottom=thin)

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
    return " + ".join(flags) if flags else ""

def get_row_color(opp: dict, known_clients: list) -> str:
    days = opp.get("days_to_close", 999)
    org  = (opp.get("organization", "") + " " + opp.get("issuing_org", "")).lower()
    is_known = any(
        any(w in org for w in c["name"].lower().split() if len(w) > 4)
        for c in known_clients
    )
    if days <= 3:
        return COLOR_URGENT
    elif days <= 7:
        return COLOR_SOON
    elif is_known:
        return COLOR_CLIENT
    elif opp.get("score", 0) >= 60:
        return COLOR_RELEVANT
    else:
        return COLOR_OK

def guess_amount(opp: dict) -> str:
    """Make educated guess at contract value."""
    import re
    duration = (opp.get("contract_duration", "") or "").lower()
    sol_type = (opp.get("solicitation_type", "") or "").lower()
    org      = (opp.get("organization", "") or "").lower()
    title    = (opp.get("title", "") or "").lower()

    # Extract years
    years = 1
    year_match = re.search(r'(\d+)\s*year', duration)
    if year_match:
        years = int(year_match.group(1))
    option_match = re.search(r'option.*?(\d+).*?year', duration)
    if option_match:
        years += int(option_match.group(1)) * 0.5

    # Base by type
    if "rfsa" in sol_type or "supply arrangement" in sol_type:
        base = 500000
    elif "rfp" in sol_type and "formal" in sol_type:
        base = 200000
    elif "rfp" in sol_type:
        base = 150000
    elif "rfq" in sol_type and "formal" in sol_type:
        base = 100000
    elif "acan" in sol_type or "npp" in sol_type:
        base = 100000
    else:
        base = 75000

    # Org multiplier
    mult = 1.0
    if any(k in org for k in ["federal", "canada", "department", "government of canada"]):
        mult = 1.5
    elif any(k in org for k in ["university", "hospital", "hydro", "power", "bank", "insurance"]):
        mult = 1.3

    # Title multiplier
    if any(k in title for k in ["enterprise", "transformation", "erp", "platform", "system"]):
        mult *= 1.4

    estimate = round(base * years * mult / 25000) * 25000
    estimate = max(estimate, 75000)
    return f"~${estimate:,.0f}"


def build_workbook(opportunities: list, known_clients: list) -> openpyxl.Workbook:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Merx Opportunities"

    scan_date  = datetime.now().strftime("%Y-%m-%d %H:%M")
    urgent     = sum(1 for o in opportunities if o.get("days_to_close", 999) <= 3)
    soon       = sum(1 for o in opportunities if 3 < o.get("days_to_close", 999) <= 7)
    known      = sum(1 for o in opportunities if "CLIENT" in build_flags(o, known_clients))
    total      = len(opportunities)

    # ── Row 1: Title ──────────────────────────────────────────────────────────
    ws.merge_cells(f"A1:{get_column_letter(len(COLUMNS))}1")
    title_cell = ws["A1"]
    title_cell.value = f"MERX OPPORTUNITY TRACKER — ALLEYNE GROUP  |  Last scan: {scan_date}"
    title_cell.font  = Font(bold=True, color="FFFFFF", size=13)
    title_cell.fill  = make_fill(COLOR_HEADER)
    title_cell.alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[1].height = 24

    # ── Row 2: Summary stats ──────────────────────────────────────────────────
    stats = [
        f"Total: {total}",
        f"Urgent (≤3 days): {urgent}",
        f"Soon (≤7 days): {soon}",
        f"Known clients: {known}",
        "", "", "", "",
        "🌸 Rose = Urgent  🌼 Yellow = Soon  🌿 Green = OK  💙 Blue = Known client  💜 Purple = Relevant"
    ]
    for col_idx, stat in enumerate(stats, 1):
        cell = ws.cell(row=2, column=col_idx, value=stat)
        cell.fill = make_fill(COLOR_SUMMARY)
        cell.font = Font(bold=True, size=10)
    ws.row_dimensions[2].height = 18

    # ── Row 3: blank separator ────────────────────────────────────────────────
    ws.row_dimensions[3].height = 6

    # ── Row 4: Column headers ─────────────────────────────────────────────────
    for col_idx, (header, width) in enumerate(COLUMNS, 1):
        cell = ws.cell(row=4, column=col_idx, value=header)
        cell.font      = Font(bold=True, color="FFFFFF", size=10)
        cell.fill      = make_fill(COLOR_HEADER)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border    = make_border()
        ws.column_dimensions[get_column_letter(col_idx)].width = width
    ws.row_dimensions[4].height = 30

    # ── Data rows ─────────────────────────────────────────────────────────────
    sorted_opps = sorted(opportunities, key=lambda x: x.get("days_to_close", 999))

    for row_idx, opp in enumerate(sorted_opps, 5):
        flags    = build_flags(opp, known_clients)
        color    = get_row_color(opp, known_clients)
        row_fill = make_fill(color)

        values = [
            flags,
            opp.get("organization", ""),
            opp.get("title", ""),
            opp.get("reference_number", ""),
            opp.get("solicitation_number", ""),
            ", ".join(opp.get("matched_capabilities", [])[:2]),
            opp.get("solicitation_type", ""),
            "New",
            "",
            guess_amount(opp),
            "",
            "",
            opp.get("closing_date", ""),
            opp.get("days_to_close", ""),
            opp.get("published_date", ""),
            opp.get("location", ""),
            opp.get("contract_duration", ""),
            opp.get("bid_intent", ""),
            (opp.get("description", "") or "")[:300],
            "",
            opp.get("contact_name", ""),
            opp.get("contact_email", ""),
            opp.get("url", ""),
            "; ".join(opp.get("document_links", [])[:3]),
            "",
            ", ".join(opp.get("agreement_types", [])),
            opp.get("qa_deadline", ""),
            opp.get("bid_submission_type", ""),
            "",
            scan_date,
            "",
            opp.get("merx_id", ""),
            opp.get("score", ""),
            ", ".join(opp.get("matched_capabilities", [])[:3]),
            ", ".join(opp.get("matched_signals", [])[:3]),
        ]

        for col_idx, value in enumerate(values, 1):
            cell = ws.cell(row=row_idx, column=col_idx, value=value)
            cell.fill      = row_fill
            cell.border    = make_border()
            cell.alignment = Alignment(vertical="top", wrap_text=True)
            cell.font      = Font(size=9)

        ws.row_dimensions[row_idx].height = 45

    # ── Freeze top 4 rows and first 2 columns ────────────────────────────────
    ws.freeze_panes = "C5"

    # ── Auto filter on header row ─────────────────────────────────────────────
    ws.auto_filter.ref = f"A4:{get_column_letter(len(COLUMNS))}4"

    return wb

def upload_to_nextcloud(wb: openpyxl.Workbook, filename: str) -> str:
    """Upload workbook to Nextcloud via WebDAV."""
    buffer = io.BytesIO()
    wb.save(buffer)
    buffer.seek(0)

    webdav_url = f"{NEXTCLOUD_URL}/remote.php/dav/files/{NEXTCLOUD_USER}/{NEXTCLOUD_FOLDER}/{filename}"
    auth       = HTTPBasicAuth(NEXTCLOUD_USER, NEXTCLOUD_PASSWORD)

    # Ensure folder exists
    folder_url = f"{NEXTCLOUD_URL}/remote.php/dav/files/{NEXTCLOUD_USER}/{NEXTCLOUD_FOLDER}"
    requests.request("MKCOL", folder_url, auth=auth)

    # Upload file
    response = requests.put(
        webdav_url,
        data=buffer.getvalue(),
        auth=auth,
        headers={"Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}
    )

    if response.status_code in [200, 201, 204]:
        file_url = f"{NEXTCLOUD_URL}/f/{SHEET_FILENAME}"
        log.info(f"Uploaded to Nextcloud: {webdav_url}")
        return webdav_url
    else:
        raise Exception(f"Nextcloud upload failed: {response.status_code} {response.text}")

def make_filename(company: str, profile_name: str, platform: str = "MerxS") -> str:
    """Generate filename: DATE_PLATFORM_Company_ProfileName.xlsx"""
    date = datetime.now().strftime("%Y-%m-%d")
    # Clean names for filename
    company_clean = company.replace(" ", "").replace(".", "").replace(",", "")[:15]
    profile_clean = profile_name.replace(" ", "-").replace("/", "-").replace("\\", "-")
    profile_clean = "".join(c for c in profile_clean if c.isalnum() or c == "-")[:30]
    return f"{date}_{platform}_{company_clean}_{profile_clean}.xlsx"

def write_to_nextcloud(opportunities: list, known_clients: list,
                       company: str = "Alleyne Inc.",
                       profile_name: str = "All-Profiles",
                       platform: str = "MerxS") -> str:
    """Build Excel file and upload to Nextcloud."""
    log.info(f"Building Excel sheet with {len(opportunities)} opportunities...")
    wb  = build_workbook(opportunities, known_clients)
    filename = make_filename(company, profile_name, platform)
    url = upload_to_nextcloud(wb, filename)
    log.info(f"Done! File available at: {url}")
    return url