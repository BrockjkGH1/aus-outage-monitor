#!/usr/bin/env python3
"""
NBN & Opticomm Hourly Outage Collector
--------------------------------------
Fetches official ACMA Major & Significant Local Outage registers (and optional
RSP JSON status feeds), filters out stale/closed historical archive entries
older than 24 hours, normalizes split date/time columns into plain 12-hour
AM/PM local Australian state times, sorts active incidents to the top, and
exports:
  1. outages.json              -> Consumed by nbn_opticomm_outage_dashboard.html
  2. outages_gem_knowledge.csv -> Linked/uploaded to a custom Gemini Gem

Zero external dependencies (uses Python 3.9+ standard library only).
"""

import csv
import io
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

# -----------------------------------------------------------------------------
# 1. CONFIGURATION & ENDPOINTS
# -----------------------------------------------------------------------------
OUTPUT_JSON_PATH = os.getenv("OUTAGE_JSON_PATH", "outages.json")
OUTPUT_CSV_PATH = os.getenv("OUTAGE_CSV_PATH", "outages_gem_knowledge.csv")
POLL_INTERVAL_SECONDS = 3600  # 1 hour (60 minutes)

# Automatically discard Resolved / past-end-date incidents older than this many hours
MAX_RESOLVED_AGE_HOURS = int(os.getenv("MAX_RESOLVED_AGE_HOURS", "24"))

# Official ACMA Outage Register pages & direct CSV API endpoints
NBN_REGISTER_PAGE = "https://www.nbnco.com.au/utility/major-significant-local-outages-register"
OPTICOMM_REGISTER_PAGE = "https://www.opticomm.com.au/outages/register/"
OPTICOMM_CSV_DIRECT_URL = "https://www.opticomm.com.au/api/network/register/?format=csv"

# Live Public Australian ISP / RSP Network Status Reference Points
# These expose real-time NBN POI/CVC outages, Opticomm estate faults, and scheduled works
AUSSIE_BB_OUTAGES_URL = "https://www.aussiebroadband.com.au/network/outages/"
LAUNTEL_OUTAGES_URL = "https://www.launtel.net.au/outages-announcements"
SUPERLOOP_OUTAGES_URL = "https://www.superloop.com/outages/"

# Optional: Comma-separated URLs of additional public RSP status JSON feeds
# Example: export RSP_STATUS_FEED_URLS="https://example-rsp.com.au/api/outages.json"
EXTRA_JSON_FEEDS = [
    u.strip() for u in os.getenv("RSP_STATUS_FEED_URLS", "").split(",") if u.strip()
]

USER_AGENT = (
    "Mozilla/5.0 (compatible; AusOutageMonitor/1.0; +https://github.com/outage-monitor)"
)

# Australian State/Territory Timezones
STATE_TIMEZONES: Dict[str, Tuple[str, str]] = {
    "WA":  ("Australia/Perth",     "AWST"),
    "SA":  ("Australia/Adelaide",  "ACDT"),
    "NT":  ("Australia/Darwin",    "ACST"),
    "QLD": ("Australia/Brisbane",  "AEST"),
    "NSW": ("Australia/Sydney",    "AEDT"),
    "VIC": ("Australia/Melbourne", "AEDT"),
    "TAS": ("Australia/Hobart",    "AEDT"),
    "ACT": ("Australia/Canberra",  "AEDT"),
}

# Postcode prefix fallback mapping if State column is missing
POSTCODE_STATE_RANGES: List[Tuple[int, int, str]] = [
    (200, 299, "ACT"),
    (800, 999, "NT"),
    (1000, 2599, "NSW"),
    (2600, 2618, "ACT"),
    (2619, 2899, "NSW"),
    (2900, 2920, "ACT"),
    (2921, 2999, "NSW"),
    (3000, 3999, "VIC"),
    (4000, 4999, "QLD"),
    (5000, 5999, "SA"),
    (6000, 6999, "WA"),
    (7000, 7999, "TAS"),
]

# Well-known NBN POI / Suburb / Estate -> State lookup for RSP status feeds
KNOWN_POI_STATES: Dict[str, str] = {
    # WA
    "applecross": "WA", "bassendean": "WA", "geraldton": "WA", "hilton": "WA",
    "balga": "WA", "alkimos": "WA", "eglinton": "WA", "ellenbrook": "WA",
    "cannington": "WA", "subiaco": "WA", "joondalup": "WA", "mandurah": "WA",
    "rockingham": "WA", "midland": "WA", "fremantle": "WA", "perth": "WA",
    "bunbury": "WA", "kalgoorlie": "WA", "albany": "WA", "broome": "WA",
    # VIC
    "caulfield": "VIC", "cranbourne": "VIC", "keysborough": "VIC", "hawthorn": "VIC",
    "karingal": "VIC", "kings park": "VIC", "mount eliza": "VIC", "port melbourne": "VIC",
    "south morang": "VIC", "bendigo": "VIC", "kew": "VIC", "north balwyn": "VIC",
    "footscray": "VIC", "shepparton": "VIC", "werribee": "VIC", "tarneit": "VIC",
    "armstrong creek": "VIC", "geelong": "VIC", "ballarat": "VIC", "melbourne": "VIC",
    "dandenong": "VIC", "frankston": "VIC", "ringwood": "VIC", "reservoir": "VIC",
    # NSW / ACT
    "sydney": "NSW", "parramatta": "NSW", "chatswood": "NSW", "newcastle": "NSW",
    "wollongong": "NSW", "oran park": "NSW", "marsden park": "NSW", "riverstone": "NSW",
    "penrith": "NSW", "liverpool": "NSW", "blacktown": "NSW", "gosford": "NSW",
    "coffs harbour": "NSW", "tamworth": "NSW", "wagga wagga": "NSW", "albury": "NSW",
    "canberra": "ACT", "gungahlin": "ACT", "belconnen": "ACT", "tuggeranong": "ACT",
    # QLD
    "aspley": "QLD", "nambour": "QLD", "nerang": "QLD", "woolloongabba": "QLD",
    "brisbane": "QLD", "gold coast": "QLD", "sunshine coast": "QLD", "townsville": "QLD",
    "cairns": "QLD", "toowoomba": "QLD", "ipswich": "QLD", "yarrabilba": "QLD",
    "springfield": "QLD", "goodna": "QLD", "southport": "QLD", "robina": "QLD",
    # SA
    "adelaide": "SA", "mount barker": "SA", "port lincoln": "SA", "gawler": "SA",
    "modbury": "SA", "marion": "SA", "elizabeth": "SA", "salisbury": "SA",
    # TAS
    "launceston": "TAS", "hobart": "TAS", "devonport": "TAS", "burnie": "TAS",
    "kingston": "TAS",
    # NT
    "darwin": "NT", "palmerston": "NT", "alice springs": "NT", "katherine": "NT",
}


# -----------------------------------------------------------------------------
# 2. HELPER FUNCTIONS (12-HOUR TIME, STATE INFERENCE & TEXT CLEANING)
# -----------------------------------------------------------------------------
def fetch_url_text(url: str, timeout: int = 25) -> str:
    """Fetches a URL using urllib with a standard User-Agent header."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return resp.read().decode(charset, errors="replace")


def discover_csv_url(page_url: str) -> Optional[str]:
    """Scrapes an ACMA register HTML page to locate the downloadable .csv file link."""
    try:
        html = fetch_url_text(page_url)
    except Exception as exc:
        print(f"[WARN] Could not fetch register page {page_url}: {exc}")
        return None

    matches = re.findall(
        r'href=["\']([^"\']*(?:\.csv|format=csv)[^"\']*)["\']',
        html,
def infer_state(raw_state: str, postcodes: List[str], location_hint: str = "") -> str:
    """Normalizes Australian state/territory code or infers it from postcode/suburb."""
    cleaned = (raw_state or "").strip().upper()
    aliases = {
        "WESTERN AUSTRALIA": "WA",
        "NEW SOUTH WALES": "NSW",
        "VICTORIA": "VIC",
        "QUEENSLAND": "QLD",
        "SOUTH AUSTRALIA": "SA",
        "TASMANIA": "TAS",
        "NORTHERN TERRITORY": "NT",
        "AUSTRALIAN CAPITAL TERRITORY": "ACT",
    }
    if cleaned in STATE_TIMEZONES:
        return cleaned
    if cleaned in aliases:
        return aliases[cleaned]

    for pc in postcodes:
        digits = re.sub(r"\D", "", pc)
        if len(digits) >= 3:
            num = int(digits[:4])
            for low, high, st in POSTCODE_STATE_RANGES:
                if low <= num <= high:
                    return st

    hint_lower = f"{raw_state} {location_hint}".lower()
    for state_abbr in STATE_TIMEZONES:
        if re.search(rf"\b{state_abbr.lower()}\b", hint_lower):
            return state_abbr

    for place, st in KNOWN_POI_STATES.items():
        if place in hint_lower:
            return st

    return "NSW"


def parse_timestamp_utc(raw_ts: str, state_code: str) -> Optional[datetime]:
    """Parses ISO-8601 or Australian CSV date/time strings into a UTC datetime."""
    if not raw_ts or not raw_ts.strip():
        return None

    text = raw_ts.strip()
    tz_name, _ = STATE_TIMEZONES.get(state_code, ("Australia/Sydney", "AEDT"))
    target_tz = ZoneInfo(tz_name)

    iso_candidate = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso_candidate)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=target_tz)
        return dt.astimezone(timezone.utc)
    except ValueError:
        pass

    patterns = [
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %I:%M %p",
        "%d/%m/%Y %I:%M:%S %p",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %I:%M %p",
        "%d-%m-%Y %H:%M",
        "%d/%m/%Y",
        "%Y-%m-%d",
    ]
    stripped_tz = re.sub(r"\s+[A-Z]{3,4}$", "", text)
    for fmt in patterns:
        try:
            dt = datetime.strptime(stripped_tz, fmt).replace(tzinfo=target_tz)
            return dt.astimezone(timezone.utc)
        except ValueError:
            continue

    return None


def format_12h_local(raw_ts: str, state_code: str) -> str:
    """
    Converts ISO-8601, UTC, or 24-hour timestamps into clean 12-hour AM/PM
    local state time (e.g., '2026-11-22 1:00 AM AWST').
    """
    if not raw_ts or not raw_ts.strip():
        return "To Be Confirmed"

    text = raw_ts.strip()
    tz_name, tz_abbr = STATE_TIMEZONES.get(state_code, ("Australia/Sydney", "AEDT"))
    target_tz = ZoneInfo(tz_name)

    dt_utc = parse_timestamp_utc(text, state_code)
    if dt_utc is not None:
        dt = dt_utc.astimezone(target_tz)
        hour_12 = dt.strftime("%I").lstrip("0") or "12"
        return f"{dt.strftime('%Y-%m-%d')} {hour_12}:{dt.strftime('%M %p')} {tz_abbr}"

    return text


def normalize_status(raw_status: str) -> str:
    """Maps carrier status strings to dashboard status categories."""
    s = (raw_status or "").strip().lower()
    if any(k in s for k in ("resolved", "restored", "closed", "completed")):
        return "Resolved"
    if any(k in s for k in ("planned", "scheduled", "maintenance", "upgrade")):
        return "Planned Maintenance"
    if any(k in s for k in ("degraded", "intermittent", "partial", "congestion")):
        return "Degraded Performance"
    return "Unplanned Outage"


def split_list_field(raw_val: str) -> List[str]:
    """Splits comma/semicolon/pipe-delimited suburb or postcode strings cleanly."""
    if not raw_val:
        return []
    parts = re.split(r"[,;|/]+", raw_val)
    return [p.strip() for p in parts if p.strip()]


# -----------------------------------------------------------------------------
# 3. CARRIER PARSERS (NBN & OPTICOMM CSV + OPTIONAL JSON FEEDS)
# -----------------------------------------------------------------------------
def parse_acma_csv(csv_text: str, carrier_name: str) -> List[Dict[str, Any]]:
    """
    Normalizes rows from NBN or Opticomm ACMA outage register CSVs into the
    exact schema expected by nbn_opticomm_outage_dashboard.html, handling
    separate date/time columns and filtering out old resolved rows.
    """
    csv_text = csv_text.lstrip("\ufeff")
    reader = csv.DictReader(io.StringIO(csv_text))
    incidents: List[Dict[str, Any]] = []
    now_utc = datetime.now(timezone.utc)
    cutoff_utc = now_utc - timedelta(hours=MAX_RESOLVED_AGE_HOURS)
    now_str = now_utc.astimezone(ZoneInfo("Australia/Perth")).strftime("%I:%M %p AWST").lstrip("0")

    for idx, row in enumerate(reader, start=1):
        lower_row = {(k or "").strip().lower(): (v or "").strip() for k, v in row.items()}

        def pick(*candidate_keys: str) -> str:
            for key in candidate_keys:
                for col_name, val in lower_row.items():
                    if key in col_name and val:
                        return val
            return ""

        raw_id = pick("reference", "incident", "outage id", "ticket", "id") or f"{carrier_name.upper()}-{idx:04d}"
        raw_title = pick("outage title", "title")
        raw_suburbs = pick("affected suburb", "suburb", "town", "locality", "affected locations", "location", "area")
        raw_postcodes = pick("affected postcode", "postcode", "post code", "zip")
        raw_state = pick("affected state", "state", "territory", "jurisdiction")
        raw_tech = pick("service type", "technology", "network", "access") or (
            "Opticomm Fibre" if carrier_name == "Opticomm" else "NBN Multi-Tech"
        )
        raw_rectified = pick("date last rectified", "rectified", "resolved date", "restored date")
        raw_status = pick("status", "state of outage", "outage type", "type")
        raw_cause = pick("cause", "reason", "description", "details") or raw_title or "Carrier network fault under investigation."
        raw_poi = pick("poi", "exchange", "headend", "node", "csa") or (raw_title if raw_title else "ACMA Register Feed")
        raw_premises = pick("premises affected", "services affected", "premises", "services", "customers", "impacted")

        # Handle ACMA split date + time columns (e.g., "Start date of outage" + "Start time of outage")
        start_date_col = pick("start date of outage", "start date", "commenced date")
        start_time_col = pick("start time of outage", "start time", "commenced time")
        if start_date_col and start_time_col and start_time_col not in start_date_col:
            raw_start = f"{start_date_col} {start_time_col}"
        else:
            raw_start = start_date_col or pick("start", "commenced", "detected", "created", "date")

        end_date_col = pick("end date of outage", "end date", "estimated end date")
        end_time_col = pick("end time of outage", "end time", "estimated end time")
        if end_date_col and end_time_col and end_time_col not in end_date_col:
            raw_end = f"{end_date_col} {end_time_col}"
        else:
            raw_end = end_date_col or pick("restoration", "etr", "estimated", "end")

        suburbs = split_list_field(raw_suburbs) or ["Suburb Details Pending"]
        postcodes = split_list_field(raw_postcodes) or ["0000"]
        state_code = infer_state(raw_state, postcodes, " ".join(suburbs))

        premises_digits = re.sub(r"\D", "", raw_premises)
        affected_premises = int(premises_digits) if premises_digits else 0

        start_dt_utc = parse_timestamp_utc(raw_start, state_code)
        end_dt_utc = parse_timestamp_utc(raw_end, state_code)
        rectified_dt_utc = parse_timestamp_utc(raw_rectified, state_code)

        # Determine true status: ACMA registers populate "Date last rectified" OR a past "End date of outage" once closed
        if raw_rectified or (end_dt_utc is not None and end_dt_utc < now_utc):
            status = "Resolved"
        else:
            status = normalize_status(raw_status)

        # Filter out historical/closed archive records older than MAX_RESOLVED_AGE_HOURS
        if status == "Resolved":
            closed_ref_dt = rectified_dt_utc or end_dt_utc or start_dt_utc
            if closed_ref_dt is not None and closed_ref_dt < cutoff_utc:
                continue

        start_12h = format_12h_local(raw_start, state_code)
        if status == "Resolved":
            resolved_ts_str = raw_rectified or raw_end
            etr_12h = f"Restored {format_12h_local(resolved_ts_str, state_code)}"
        else:
            etr_12h = format_12h_local(raw_end, state_code)

        incidents.append({
            "id": raw_id,
            "carrier": carrier_name,
            "status": status,
            "state": state_code,
            "suburbs": suburbs,
            "postcodes": postcodes,
            "technology": raw_tech,
            "poiOrExchange": raw_poi,
            "cause": raw_cause,
            "summary": f"{carrier_name} {status.lower()} affecting {', '.join(suburbs[:4])} ({state_code}).",
            "startTime": start_12h,
            "etr": etr_12h,
            "affectedPremises": affected_premises,
            "_sortTs": start_dt_utc.timestamp() if start_dt_utc else 0.0,
            "updates": [
                {
                    "time": now_str,
                    "note": f"Synchronized from official {carrier_name} ACMA Outage Register."
                }
            ]
        })

    return incidents


def strip_html_to_lines(html_text: str) -> List[str]:
    """Converts HTML page markup into clean non-empty lines of text."""
    cleaned = re.sub(r"<script[^>]*>.*?</script>", " ", html_text, flags=re.DOTALL | re.IGNORECASE)
    cleaned = re.sub(r"<style[^>]*>.*?</style>", " ", cleaned, flags=re.DOTALL | re.IGNORECASE)
    cleaned = re.sub(r"<[^>]+>", "\n", cleaned)
    cleaned = (
        cleaned.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&quot;", '"')
        .replace("&#39;", "'")
        .replace("&ndash;", "-")
        .replace("&mdash;", "-")
    )
    return [line.strip() for line in cleaned.splitlines() if line.strip()]


def parse_aussie_bb_status(html_text: str) -> List[Dict[str, Any]]:
    """
    Parses Aussie Broadband's live public outages & scheduled maintenance page,
    which publishes granular NBN CVC/POI and Opticomm network events with
    exact suburb/POI names and start/end windows.
    """
    records: List[Dict[str, Any]] = []
    now_awst = datetime.now(ZoneInfo("Australia/Perth"))
    now_str = now_awst.strftime("%I:%M %p AWST").lstrip("0")
    current_year = now_awst.year

    # First check if Aussie BB embeds structured JSON state/props in the page
    json_blobs = re.findall(r"(\[\s*\{.*?\}\s*\])", html_text, flags=re.DOTALL)
    for blob in json_blobs:
        if "CVC" not in blob and "Opticomm" not in blob and "outage" not in blob.lower():
            continue
        try:
            items = json.loads(blob)
            if isinstance(items, list) and items and isinstance(items[0], dict):
                for idx, obj in enumerate(items, start=1):
                    title = str(obj.get("title") or obj.get("name") or obj.get("type") or "")
                    areas = obj.get("affected_areas") or obj.get("areas") or []
                    if not title and not areas:
                        continue
                    area_list = [str(a) for a in areas] if isinstance(areas, list) else split_list_field(str(areas))
                    joined = f"{title} {' '.join(area_list)}".lower()
                    if "mobile" in joined and "nbn" not in joined and "opticomm" not in joined:
                        continue
                    carrier = "Opticomm" if "opticomm" in joined else "NBN"
                    suburbs = [
                        re.sub(r"\s*\(Link\s*\d+\)", "", a.replace("NBN CVC:", "").replace("Depot", "")).strip()
                        for a in area_list
                        if not a.lower().startswith("wholesalenbncvc")
                    ] or ["Multiple POIs"]
                    state_code = infer_state("", [], " ".join(suburbs))
                    status = normalize_status(title)
                    start_raw = str(obj.get("start") or obj.get("start_date") or "")
                    end_raw = str(obj.get("end") or obj.get("expected_end") or "")
                    records.append({
                        "id": f"ABB-{obj.get('id', idx)}",
                        "carrier": carrier,
                        "status": status,
                        "state": state_code,
                        "suburbs": suburbs[:8],
                        "postcodes": ["POI/CVC"],
                        "technology": "Opticomm Fibre" if carrier == "Opticomm" else "NBN CVC / POI",
                        "poiOrExchange": ", ".join(area_list[:4]) or "Aussie Broadband Network",
                        "cause": title or "RSP Wholesale Backhaul / CVC Event",
                        "summary": f"Reported via Aussie Broadband NOC feed affecting {', '.join(suburbs[:4])}.",
                        "startTime": format_12h_local(start_raw, state_code) if start_raw else now_str,
                        "etr": format_12h_local(end_raw, state_code) if end_raw else "To Be Confirmed",
                        "affectedPremises": 0,
                        "updates": [{"time": now_str, "note": "Ingested from Aussie Broadband Live Status Feed."}],
                    })
                if records:
                    return records
        except Exception:
            pass

    # Fallback: parse clean text blocks between "Current outages" and "Past outages"
    lines = strip_html_to_lines(html_text)
    in_active_section = False
    block_lines: List[str] = []
    blocks: List[List[str]] = []

    for line in lines:
        low = line.lower()
        if low in ("current outages", "upcoming outages"):
            in_active_section = True
            continue
        if low == "past outages":
            if block_lines:
                blocks.append(block_lines)
            break
        if not in_active_section:
            continue

        if low in ("network maintenance", "unscheduled outage", "planned outage", "outage", "degraded performance"):
            if block_lines:
                blocks.append(block_lines)
            block_lines = [line]
        elif block_lines:
            block_lines.append(line)

    for idx, blk in enumerate(blocks, start=1):
        blk_text = " ".join(blk)
        low_text = blk_text.lower()
        # Skip mobile-only outages
        if "mobile phones" in low_text and "nbn" not in low_text and "opticomm" not in low_text:
            continue

        carrier = "Opticomm" if "opticomm" in low_text else "NBN"
        status = normalize_status(blk[0])

        # Extract NBN CVC / POI / Suburb names
        raw_areas = [
            ln for ln in blk
            if ln.startswith("NBN CVC:") or "Opticomm" in ln or re.match(r"^[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s*\(\d{4}\)$", ln)
        ]
        suburbs = [
            re.sub(r"\s*\(Link\s*\d+\)", "", a.replace("NBN CVC:", "").replace("Depot", "")).strip()
            for a in raw_areas
        ]
        postcodes = re.findall(r"\((\d{4})\)", blk_text) or ["POI/CVC"]
        if not suburbs:
            suburbs = ["Regional / Metro POI"]

        state_code = infer_state("", postcodes, " ".join(suburbs))

        # Parse Start / Expected end lines (e.g., "22 November 9:00 am")
        start_str = "To Be Confirmed"
        etr_str = "To Be Confirmed"
        for i, ln in enumerate(blk):
            if ln.lower() == "start" and i + 1 < len(blk):
                dt_match = re.match(r"(\d{1,2}\s+[A-Za-z]+\s+\d{1,2}:\d{2}\s*(?:am|pm))", blk[i + 1], re.IGNORECASE)
                if dt_match:
                    try:
                        parsed_dt = datetime.strptime(f"{dt_match.group(1)} {current_year}", "%d %B %I:%M %p %Y")
                        start_str = format_12h_local(parsed_dt.strftime("%Y-%m-%d %H:%M"), state_code)
                    except ValueError:
                        start_str = dt_match.group(1)
            if ln.lower() == "expected end" and i + 1 < len(blk):
                dt_match = re.match(r"(\d{1,2}\s+[A-Za-z]+\s+\d{1,2}:\d{2}\s*(?:am|pm))", blk[i + 1], re.IGNORECASE)
                if dt_match:
                    try:
                        parsed_dt = datetime.strptime(f"{dt_match.group(1)} {current_year}", "%d %B %I:%M %p %Y")
                        etr_str = format_12h_local(parsed_dt.strftime("%Y-%m-%d %H:%M"), state_code)
                    except ValueError:
                        etr_str = dt_match.group(1)

        records.append({
            "id": f"ABB-{now_awst.strftime('%m%d')}-{idx:02d}",
            "carrier": carrier,
            "status": status,
            "state": state_code,
            "suburbs": list(dict.fromkeys(suburbs))[:8],
            "postcodes": list(dict.fromkeys(postcodes)),
            "technology": "Opticomm Fibre" if carrier == "Opticomm" else "NBN CVC / POI",
            "poiOrExchange": ", ".join(raw_areas[:4]) if raw_areas else "Aussie Broadband CVC",
            "cause": f"{blk[0]} reported on Aussie Broadband network status feed.",
            "summary": f"{carrier} {status.lower()} affecting {', '.join(list(dict.fromkeys(suburbs))[:4])} ({state_code}).",
            "startTime": start_str,
            "etr": etr_str,
            "affectedPremises": 0,
            "updates": [{"time": now_str, "note": "Synchronized from Aussie Broadband Live Status Feed."}],
        })

    return records


def parse_launtel_status(html_text: str) -> List[Dict[str, Any]]:
    """
    Parses Launtel's live Outages & Announcements page for active NBN,
    Opticomm, and Redtrain incidents.
    """
    records: List[Dict[str, Any]] = []
    lines = strip_html_to_lines(html_text)
    now_str = datetime.now(ZoneInfo("Australia/Perth")).strftime("%I:%M %p AWST").lstrip("0")

    in_active = False
    for idx, line in enumerate(lines):
        low = line.lower()
        if low in ("current outages", "upcoming maintenance"):
            in_active = True
            continue
        if low.startswith("recent outages"):
            break
        if not in_active or "no current outages" in low or "no upcoming maintenance" in low:
            continue

        if any(k in low for k in ("nbn", "opticomm", "redtrain", "outage", "maintenance")):
            carrier = "Opticomm" if ("opticomm" in low or "redtrain" in low) else "NBN"
            state_code = infer_state("", [], line)
            status = normalize_status(line)
            records.append({
                "id": f"LAU-{idx:03d}",
                "carrier": carrier,
                "status": status,
                "state": state_code,
                "suburbs": [line[:60]],
                "postcodes": ["RSP-FEED"],
                "technology": "Opticomm Fibre" if carrier == "Opticomm" else "NBN Multi-Tech",
                "poiOrExchange": "Launtel Upstream Feed",
                "cause": line,
                "summary": f"Active alert reported on Launtel NOC status board: {line}",
                "startTime": now_str,
                "etr": "To Be Confirmed",
                "affectedPremises": 0,
                "updates": [{"time": now_str, "note": "Synchronized from Launtel Outages & Announcements."}],
            })

    return records


def collect_all_outages() -> List[Dict[str, Any]]:
    """Collects, normalizes, deduplicates, and sorts NBN and Opticomm outage data."""
    all_records: List[Dict[str, Any]] = []

    # 1. Fetch NBN ACMA Register CSV
    nbn_csv_url = os.getenv("NBN_CSV_OVERRIDE_URL") or discover_csv_url(NBN_REGISTER_PAGE)
    if nbn_csv_url:
        try:
            print(f"[INFO] Fetching NBN CSV: {nbn_csv_url}")
            nbn_csv = fetch_url_text(nbn_csv_url)
            nbn_items = parse_acma_csv(nbn_csv, "NBN")
            print(f"[INFO] Parsed {len(nbn_items)} current NBN ACMA records.")
            all_records.extend(nbn_items)
        except Exception as exc:
            print(f"[ERROR] Failed parsing NBN CSV: {exc}")
    else:
        print("[WARN] No NBN CSV link discovered on register page.")

    # 2. Fetch Opticomm ACMA Register CSV
    opti_csv_url = (
        os.getenv("OPTICOMM_CSV_OVERRIDE_URL")
        or OPTICOMM_CSV_DIRECT_URL
        or discover_csv_url(OPTICOMM_REGISTER_PAGE)
    )
    if opti_csv_url:
        try:
            print(f"[INFO] Fetching Opticomm CSV: {opti_csv_url}")
            opti_csv = fetch_url_text(opti_csv_url)
            opti_items = parse_acma_csv(opti_csv, "Opticomm")
            print(f"[INFO] Parsed {len(opti_items)} current Opticomm ACMA records.")
            all_records.extend(opti_items)
        except Exception as exc:
            print(f"[ERROR] Failed parsing Opticomm CSV: {exc}")
    else:
        print("[WARN] No Opticomm CSV link discovered on register page.")

    # 3. Fetch Aussie Broadband Live Status Feed (NBN CVC/POI & Opticomm)
    try:
        print(f"[INFO] Fetching Aussie Broadband Status: {AUSSIE_BB_OUTAGES_URL}")
        abb_html = fetch_url_text(AUSSIE_BB_OUTAGES_URL)
        abb_items = parse_aussie_bb_status(abb_html)
        print(f"[INFO] Parsed {len(abb_items)} active/upcoming Aussie Broadband records.")
        all_records.extend(abb_items)
    except Exception as exc:
        print(f"[WARN] Could not parse Aussie Broadband status page: {exc}")

    # 4. Fetch Launtel Live Outages & Announcements Feed
    try:
        print(f"[INFO] Fetching Launtel Status: {LAUNTEL_OUTAGES_URL}")
        lau_html = fetch_url_text(LAUNTEL_OUTAGES_URL)
        lau_items = parse_launtel_status(lau_html)
        print(f"[INFO] Parsed {len(lau_items)} active Launtel records.")
        all_records.extend(lau_items)
    except Exception as exc:
        print(f"[WARN] Could not parse Launtel status page: {exc}")

    # 5. Fetch any optional custom RSP JSON feeds
    for feed_url in EXTRA_JSON_FEEDS:
        try:
            print(f"[INFO] Fetching supplementary JSON feed: {feed_url}")
            raw_json = fetch_url_text(feed_url)
            parsed = json.loads(raw_json)
            if isinstance(parsed, list):
                all_records.extend(parsed)
        except Exception as exc:
            print(f"[ERROR] Failed fetching supplementary feed {feed_url}: {exc}")

    # Deduplicate by Incident ID while preserving latest record
    deduped: Dict[str, Dict[str, Any]] = {}
    for rec in all_records:
        deduped[rec["id"]] = rec

    # Sort active/in-progress incidents first, newest start time at the top
    status_priority = {
        "Unplanned Outage": 0,
        "Degraded Performance": 1,
        "Planned Maintenance": 2,
        "Resolved": 3,
    }
    sorted_records = sorted(
        deduped.values(),
        key=lambda r: (status_priority.get(r.get("status", ""), 9), -r.get("_sortTs", 0.0))
    )

    for rec in sorted_records:
        rec.pop("_sortTs", None)

    return sorted_records


# -----------------------------------------------------------------------------
# 4. EXPORTERS (JSON FOR DASHBOARD + CSV FOR GEMINI GEM KNOWLEDGE)
# -----------------------------------------------------------------------------
def write_outputs(records: List[Dict[str, Any]]) -> None:
    """Writes normalized JSON for the web dashboard and CSV for a Gemini Gem."""
    with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as jf:
        json.dump(records, jf, indent=2, ensure_ascii=False)
    print(f"[OK] Saved {len(records)} current records to {OUTPUT_JSON_PATH}")

    fieldnames = [
        "id",
        "carrier",
        "status",
        "state",
        "suburbs",
        "postcodes",
        "technology",
        "poiOrExchange",
        "startTime",
        "etr",
        "affectedPremises",
        "cause",
    ]
    with open(OUTPUT_CSV_PATH, "w", newline="", encoding="utf-8") as cf:
        writer = csv.DictWriter(cf, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            writer.writerow({
                "id": r.get("id", ""),
                "carrier": r.get("carrier", ""),
                "status": r.get("status", ""),
                "state": r.get("state", ""),
                "suburbs": "; ".join(r.get("suburbs", [])),
                "postcodes": "; ".join(r.get("postcodes", [])),
                "technology": r.get("technology", ""),
                "poiOrExchange": r.get("poiOrExchange", ""),
                "startTime": r.get("startTime", ""),
                "etr": r.get("etr", ""),
                "affectedPremises": r.get("affectedPremises", 0),
                "cause": r.get("cause", ""),
            })
    print(f"[OK] Saved Gem knowledge table to {OUTPUT_CSV_PATH}")


# -----------------------------------------------------------------------------
# 5. ENTRYPOINT (SINGLE RUN OR CONTINUOUS HOURLY DAEMON)
# -----------------------------------------------------------------------------
def main() -> None:
    loop_mode = "--loop" in os.sys.argv
    while True:
        print(f"\n=== Syncing NBN & Opticomm Outages ({datetime.now().strftime('%I:%M %p')}) ===")
        records = collect_all_outages()
        write_outputs(records)

        if not loop_mode:
            break
        print(f"[SLEEP] Waiting {POLL_INTERVAL_SECONDS // 60} minutes until next hourly check...")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
