"""
Google Sheets writer for Merx Scanner results.
Creates/updates the opportunity tracking sheet.
"""

import json
import os
import logging
from datetime import datetime, date
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

log = logging.getLogger(__name__)

# Sheet configuration
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

SHEET_NAME = "Merx Opportunities — Alleyne Group"
FOLDER_NAME = "AlleyneAdmAgent"

# Column headers matching Joel's requested format
HEADERS = [
    "Flags",
    "Client / Account",
    "Opportunity (Project)",
    "Reference Number",
    "Solicitation Number",
    "Category / UNSPSC",
    "Solicitation Type",
    "Sales Stage",
    "Amount (Est.)",
    "AI Amount Guess",
    "Probability %",
    "Weighted Value",
    "Closing Date",
    "Days Left",
    "Published Date",
    "Geographic Location",
    "Contract Duration",
    "Bid Intent",
    "Quick Summary",
    "Requirements of Proposal",
    "Contact Name",
    "Contact Email",
    "RFP Website",
    "Linked Documents",
    "Organization Website",
    "Agreement Types",
    "Q&A Deadline",
    "Bid Submission Type",
    "RFP Questions",
    "Date Found",
    "Reasons for Passing",
    "Merx ID",
    "Relevance Score",
    "Matched Capabilities",
    "Matched Signals"
]

# Color definitions (light pastels as requested)
COLORS = {
    "urgent":   {"red": 1.0,  "green": 0.8,  "blue": 0.8},   # light rose — 3 days
    "soon":     {"red": 1.0,  "green": 0.95, "blue": 0.7},   # light yellow — 7 days
    "ok":       {"red": 0.85, "green": 0.95, "blue": 0.85},  # light green — 7+ days
    "client":   {"red": 0.8,  "green": 0.9,  "blue": 1.0},   # light blue — known client
    "relevant": {"red": 0.9,  "green": 0.85, "blue": 1.0},   # light purple — high relevance
    "white":    {"red": 1.0,  "green": 1.0,  "blue": 1.0},   # white — default
    "header":   {"red": 0.27, "green": 0.35, "blue": 0.45},  # dark header
    "summary":  {"red": 0.95, "green": 0.95, "blue": 0.98},  # very light gray for summary
}


def get_client():
    """Authenticate with Google Sheets API."""
    creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "/app/google_credentials.json")
    creds = Credentials.from_service_account_file(creds_file, scopes=SCOPES)
    return gspread.authorize(creds)


def get_or_create_sheet(client, sheet_name: str):
    """Get existing sheet or create new one."""
    try:
        sheet = client.open(sheet_name)
        log.info(f"Found existing sheet: {sheet_name}")
        return sheet
    except gspread.SpreadsheetNotFound:
        log.info(f"Creating new sheet: {sheet_name}")
        sheet = client.create(sheet_name)
        # Move to shared folder if it exists
        try:
            folder_id = os.getenv("GOOGLE_FOLDER_ID", "")
            if folder_id:
                drive_service = client.auth.authorized_session
                sheet.share(None, perm_type='anyone', role='writer')
        except Exception as e:
            log.warning(f"Could not move to folder: {e}")
        return sheet


def build_flags(opp: dict, known_clients: list) -> str:
    """Build flags string for an opportunity."""
    flags = []
    days = opp.get("days_to_close", 999)

    if days <= 3:
        flags.append("URGENT")
    elif days <= 7:
        flags.append("SOON")

    # Check known clients
    org = opp.get("organization", "").lower()
    issuing = opp.get("issuing_org", "").lower()
    for client in known_clients:
        name = client["name"].lower()
        if name in org or name in issuing or any(w in org for w in name.split() if len(w) > 4):
            flag = "CLIENT★" if client.get("won") else "CLIENT"
            flags.append(flag)
            break

    if opp.get("score", 0) >= 60:
        flags.append("HOT")

    return " + ".join(flags) if flags else ""


def get_row_color(opp: dict, known_clients: list) -> dict:
    """Determine row background color."""
    days = opp.get("days_to_close", 999)
    org = opp.get("organization", "").lower()

    is_known = any(
        c["name"].lower() in org or
        any(w in org for w in c["name"].lower().split() if len(w) > 4)
        for c in known_clients
    )

    if days <= 3:
        return COLORS["urgent"]
    elif days <= 7:
        return COLORS["soon"]
    elif is_known:
        return COLORS["client"]
    elif opp.get("score", 0) >= 60:
        return COLORS["relevant"]
    else:
        return COLORS["ok"]


def guess_amount(opp: dict) -> str:
    """Make educated guess at contract value based on duration and type."""
    duration_text = opp.get("contract_duration", "").lower()
    sol_type = opp.get("solicitation_type", "").lower()

    years = 1
    if "5 year" in duration_text:
        years = 5
    elif "3 year" in duration_text:
        years = 3
    elif "2 year" in duration_text:
        years = 2

    base = 100000
    if "rfp" in sol_type:
        base = 150000
    elif "rfq" in sol_type:
        base = 75000

    guess = base * years
    return f"~${guess:,.0f} (AI est.)"


def write_to_sheet(opportunities: list, known_clients: list, scan_meta: dict):
    """Write opportunities to Google Sheet."""
    gc = get_client()
    spreadsheet = get_or_create_sheet(gc, SHEET_NAME)
    ws = spreadsheet.sheet1
    ws.clear()

    # ── Summary header rows ──────────────────────────────────────────────────
    scan_date = datetime.now().strftime("%Y-%m-%d %H:%M")
    urgent    = sum(1 for o in opportunities if o.get("days_to_close", 999) <= 3)
    soon      = sum(1 for o in opportunities if 3 < o.get("days_to_close", 999) <= 7)
    total     = len(opportunities)

    summary_rows = [
        ["MERX OPPORTUNITY TRACKER — ALLEYNE GROUP", "", f"Last scan: {scan_date}"],
        [f"Total: {total}", f"Urgent (≤3d): {urgent}", f"Soon (≤7d): {soon}",
         f"Known clients: {sum(1 for o in opportunities if 'CLIENT' in build_flags(o, known_clients))}"],
        [],  # blank separator
        HEADERS  # column headers
    ]

    all_rows = summary_rows.copy()

    # ── Data rows ─────────────────────────────────────────────────────────────
    row_colors = []
    for opp in opportunities:
        flags = build_flags(opp, known_clients)
        color = get_row_color(opp, known_clients)
        row_colors.append(color)

        row = [
            flags,
            opp.get("organization", ""),
            opp.get("title", ""),
            opp.get("reference_number", ""),
            opp.get("solicitation_number", ""),
            ", ".join(opp.get("matched_capabilities", [])[:2]) or opp.get("category", ""),
            opp.get("solicitation_type", ""),
            "New",  # Sales Stage — human updates
            "",     # Amount — human fills
            guess_amount(opp),
            "",     # Probability % — human fills
            "",     # Weighted Value — human fills
            opp.get("closing_date", ""),
            str(opp.get("days_to_close", "")),
            opp.get("published_date", ""),
            opp.get("location", ""),
            opp.get("contract_duration", ""),
            opp.get("bid_intent", ""),
            opp.get("description", "")[:300] if opp.get("description") else "",
            "",     # Requirements — human fills
            opp.get("contact_name", ""),
            opp.get("contact_email", ""),
            opp.get("url", ""),
            "; ".join(opp.get("document_links", [])[:3]),
            "",     # Organization website — human fills
            ", ".join(opp.get("agreement_types", [])),
            opp.get("qa_deadline", ""),
            opp.get("bid_submission_type", ""),
            "",     # RFP Questions — human fills
            scan_date,
            "",     # Reasons for Passing — human fills
            opp.get("merx_id", ""),
            str(opp.get("score", "")),
            ", ".join(opp.get("matched_capabilities", [])[:3]),
            ", ".join(opp.get("matched_signals", [])[:3]),
        ]
        all_rows.append(row)

    # ── Write all rows at once ────────────────────────────────────────────────
    ws.update("A1", all_rows)

    # ── Formatting ────────────────────────────────────────────────────────────
    requests = []

    # Header row formatting (row 4 = index 3)
    requests.append({
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 3, "endRowIndex": 4},
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": COLORS["header"],
                    "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "bold": True},
                    "horizontalAlignment": "CENTER"
                }
            },
            "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
        }
    })

    # Summary rows formatting (rows 1-2)
    requests.append({
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 2},
            "cell": {"userEnteredFormat": {"backgroundColor": COLORS["summary"],
                                           "textFormat": {"bold": True}}},
            "fields": "userEnteredFormat(backgroundColor,textFormat)"
        }
    })

    # Data row colors (starting row 5 = index 4)
    for i, color in enumerate(row_colors):
        row_idx = i + 4
        requests.append({
            "repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": row_idx, "endRowIndex": row_idx + 1},
                "cell": {"userEnteredFormat": {"backgroundColor": color}},
                "fields": "userEnteredFormat(backgroundColor)"
            }
        })

    # Freeze top 4 rows and first column
    requests.append({
        "updateSheetProperties": {
            "properties": {"sheetId": ws.id, "gridProperties": {"frozenRowCount": 4, "frozenColumnCount": 1}},
            "fields": "gridProperties(frozenRowCount,frozenColumnCount)"
        }
    })

    # Auto-resize columns
    requests.append({
        "autoResizeDimensions": {
            "dimensions": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": len(HEADERS)}
        }
    })

    spreadsheet.batch_update({"requests": requests})
    log.info(f"Sheet updated: {spreadsheet.url}")
    return spreadsheet.url
