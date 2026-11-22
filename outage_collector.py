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

# Unblocked Atlassian Statuspage JSON APIs for major Australian NBN/Opticomm RSPs
STATUSPAGE_ENDPOINTS: List[Tuple[str, str]] = [
    ("Superloop", "https://status.superloop.com/api/v2/summary.json"),
    ("Swoop", "https://status.swoop.com.au/api/v2/summary.json"),
    ("Exetel", "https://status.exetel.com.au/api/v2/summary.json"),
]

# Optional: Comma-separated URLs of additional public RSP status JSON feeds
# Example: export RSP_STATUS_FEED_URLS="https://example-rsp.com.au/api/outages.json"
EXTRA_JSON_FEEDS = [
    u.strip() for u in os.getenv("RSP_STATUS_FEED_URLS", "").split(",") if u.strip()
]

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/129.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,text/csv,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
    "Cache-Control": "no-cache",
}

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
def fetch_url_text(url: str, timeout: int = 30, use_reader_first: bool = False) -> str:
    """
    Fetches a URL using urllib with desktop browser headers.
    For JS-rendered pages or WAF-protected sites on GitHub Actions IPs,
    uses Jina Reader (r.jina.ai) which executes page JS and returns clean text.
    """
    urls_to_try = []
    reader_url = f"https://r.jina.ai/{url}"
    if use_reader_first:
        urls_to_try = [reader_url, url]
    else:
        urls_to_try = [url, reader_url]

    last_err: Optional[Exception] = None
    for target in urls_to_try:
        try:
            headers = dict(BROWSER_HEADERS)
            if "r.jina.ai" in target:
                headers["X-Return-Format"] = "markdown"
            req = urllib.request.Request(target, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                charset = resp.headers.get_content_charset() or "utf-8"
                body = resp.read().decode(charset, errors="replace")
                if body and len(body.strip()) > 40:
                    return body
        except Exception as exc:
            last_err = exc
            print(f"[INFO] Fetch attempt failed for {target}: {exc}")

    if last_err:
        raise last_err
    return ""


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
        flags=re.IGNORECASE,
    )
    if not matches:
        return None
    return urllib.parse.urljoin(page_url, matches[0])


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
    Parses Aussie Broadband's live public outages & scheduled maintenance page
    (via Next.js __NEXT_DATA__ JSON or structured HTML text), capturing NBN
    CVC/POI and Opticomm network events with suburb/POI names and 12h windows.
    """
    records: List[Dict[str, Any]] = []
    now_awst = datetime.now(ZoneInfo("Australia/Perth"))
    now_str = now_awst.strftime("%I:%M %p AWST").lstrip("0")
    current_year = now_awst.year

    # 1. Extract from Next.js __NEXT_DATA__ JSON payload if present
    next_data_match = re.search(
        r'<script[^>]*id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
        html_text,
        flags=re.DOTALL | re.IGNORECASE,
    )
    if next_data_match:
        try:
            next_json = json.loads(next_data_match.group(1))
            candidate_lists: List[List[Dict[str, Any]]] = []

            def walk_json(node: Any) -> None:
                if isinstance(node, dict):
                    for v in node.values():
                        walk_json(v)
                elif isinstance(node, list):
                    if node and all(isinstance(x, dict) for x in node):
                        sample_keys = {k.lower() for k in node[0].keys()}
                        if sample_keys & {"affectedareas", "affected_areas", "servicesaffected", "services_affected", "expectedend", "expected_end"}:
                            candidate_lists.append(node)
                    for item in node:
                        walk_json(item)

            walk_json(next_json)

            for item_list in candidate_lists:
                for idx, obj in enumerate(item_list, start=1):
                    status_raw = str(obj.get("status") or obj.get("type") or obj.get("category") or "")
                    if "completed" in status_raw.lower() or "past" in status_raw.lower():
                        continue
                    services_raw = obj.get("servicesAffected") or obj.get("services_affected") or obj.get("services") or ""
                    services_str = ", ".join(services_raw) if isinstance(services_raw, list) else str(services_raw)
                    areas_raw = obj.get("affectedAreas") or obj.get("affected_areas") or obj.get("areas") or []
                    area_list = [str(a) for a in areas_raw] if isinstance(areas_raw, list) else split_list_field(str(areas_raw))

                    joined = f"{status_raw} {services_str} {' '.join(area_list)}".lower()
                    if not area_list and not services_str:
                        continue
                    if "mobile" in joined and "nbn" not in joined and "opticomm" not in joined:
                        continue

                    carrier = "Opticomm" if "opticomm" in joined else "NBN"
                    suburbs = [
                        re.sub(r"\s*\(Link\s*\d+\)", "", a.replace("NBN CVC:", "").replace("Depot", "")).strip()
                        for a in area_list
                        if not a.lower().startswith("wholesalenbncvc") and not a.startswith("+")
                    ] or ["Metro / Regional POI"]
                    postcodes = re.findall(r"\((\d{4})\)", " ".join(area_list)) or ["POI/CVC"]
                    state_code = infer_state("", postcodes, " ".join(suburbs))
                    status = normalize_status(status_raw or str(obj.get("title") or "Network Maintenance"))

                    start_raw = str(obj.get("start") or obj.get("startDate") or obj.get("start_date") or "")
                    end_raw = str(obj.get("expectedEnd") or obj.get("expected_end") or obj.get("end") or "")
                    desc_raw = str(obj.get("description") or obj.get("summary") or obj.get("title") or f"{status} affecting {services_str or carrier}.")

                    records.append({
                        "id": f"ABB-{obj.get('id', f'{now_awst.strftime(\"%m%d\")}-{idx:02d}')}",
                        "carrier": carrier,
                        "status": status,
                        "state": state_code,
                        "suburbs": list(dict.fromkeys(suburbs))[:8],
                        "postcodes": list(dict.fromkeys(postcodes)),
                        "technology": "Opticomm Fibre" if carrier == "Opticomm" else "NBN CVC / POI",
                        "poiOrExchange": ", ".join(area_list[:4]) or "Aussie Broadband Network",
                        "cause": re.sub(r"<[^>]+>", " ", desc_raw).strip(),
                        "summary": f"{carrier} {status.lower()} affecting {', '.join(list(dict.fromkeys(suburbs))[:4])} ({state_code}).",
                        "startTime": format_12h_local(start_raw, state_code) if start_raw else now_str,
                        "etr": format_12h_local(end_raw, state_code) if end_raw else "To Be Confirmed",
                        "affectedPremises": 0,
                        "updates": [{"time": now_str, "note": "Synchronized from Aussie Broadband Live Status Feed."}],
                    })
            if records:
                return records
        except Exception as exc:
            print(f"[WARN] Next.js JSON parse fallback on Aussie BB: {exc}")

    # 2. Fallback: parse clean text blocks between "Current outages" and "Past outages"
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

        if any(k == low or low.startswith(k) for k in ("network maintenance", "unscheduled outage", "planned outage", "degraded performance")):
            if block_lines:
                blocks.append(block_lines)
            block_lines = [line]
        elif block_lines:
            block_lines.append(line)

    if block_lines and block_lines not in blocks:
        blocks.append(block_lines)

    for idx, blk in enumerate(blocks, start=1):
        blk_text = " ".join(blk)
        low_text = blk_text.lower()
        if "mobile phones" in low_text and "nbn" not in low_text and "opticomm" not in low_text:
            continue

        carrier = "Opticomm" if "opticomm" in low_text else "NBN"
        status = normalize_status(blk[0])

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
        if "current outages" in low or "upcoming maintenance" in low:
            in_active = True
            continue
        if "recent outages" in low:
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


def parse_statuspage_summary(rsp_name: str, raw_json_text: str) -> List[Dict[str, Any]]:
    """
    Parses Atlassian Statuspage /api/v2/summary.json feeds (Superloop, Swoop, Exetel)
    for active NBN & Opticomm incidents and scheduled maintenances.
    """
    records: List[Dict[str, Any]] = []
    try:
        payload = json.loads(raw_json_text)
    except Exception:
        return records

    now_str = datetime.now(ZoneInfo("Australia/Perth")).strftime("%I:%M %p AWST").lstrip("0")
    all_events = []
    for inc in payload.get("incidents", []) or []:
        all_events.append(("incident", inc))
    for maint in payload.get("scheduled_maintenances", []) or []:
        if str(maint.get("status", "")).lower() in ("scheduled", "in_progress", "verifying"):
            all_events.append(("maintenance", maint))

    for kind, ev in all_events:
        name = str(ev.get("name") or "")
        updates_list = ev.get("incident_updates") or []
        latest_body = str(updates_list[0].get("body") if updates_list else "")
        combined = f"{name} {latest_body}".lower()

        # Focus on NBN, Opticomm, or regional/metro backhaul faults
        if not any(k in combined for k in ("nbn", "opticomm", "poi", "cvc", "fibre", "fttp", "fttn", "hfc", "outage", "maintenance")):
            continue

        carrier = "Opticomm" if "opticomm" in combined else "NBN"
        postcodes = re.findall(r"\b(\d{4})\b", f"{name} {latest_body}")
        state_code = infer_state("", postcodes, f"{name} {latest_body}")
        status = "Planned Maintenance" if kind == "maintenance" else normalize_status(str(ev.get("status") or name))

        # Extract suburb/POI names from title
        suburb_hint = re.sub(r"^(?:NBN|Opticomm)\s*[-:]\s*", "", name, flags=re.IGNORECASE).strip()
        suburbs = [suburb_hint[:65]] if suburb_hint else ["Metro / Regional POI"]

        start_raw = str(ev.get("scheduled_for") or ev.get("started_at") or ev.get("created_at") or "")
        end_raw = str(ev.get("scheduled_until") or "")

        formatted_updates = []
        for u in updates_list[:4]:
            u_ts = format_12h_local(str(u.get("created_at") or ""), state_code)
            formatted_updates.append({
                "time": u_ts,
                "note": str(u.get("body") or "").strip()
            })
        if not formatted_updates:
            formatted_updates.append({"time": now_str, "note": f"Synchronized from {rsp_name} Live Status API."})

        records.append({
            "id": f"{rsp_name[:3].upper()}-{ev.get('id', '000')}",
            "carrier": carrier,
            "status": status,
            "state": state_code,
            "suburbs": suburbs,
            "postcodes": list(dict.fromkeys(postcodes)) or ["RSP-API"],
            "technology": "Opticomm Fibre" if carrier == "Opticomm" else "NBN Multi-Tech",
            "poiOrExchange": f"{rsp_name} Status Feed",
            "cause": name,
            "summary": latest_body[:260] if latest_body else name,
            "startTime": format_12h_local(start_raw, state_code) if start_raw else now_str,
            "etr": format_12h_local(end_raw, state_code) if end_raw else "To Be Confirmed",
            "affectedPremises": 0,
            "updates": formatted_updates,
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

    # 3. Fetch Aussie Broadband Live Status Feed via JS-rendering reader
    try:
        print(f"[INFO] Fetching Aussie Broadband Status: {AUSSIE_BB_OUTAGES_URL}")
        abb_html = fetch_url_text(AUSSIE_BB_OUTAGES_URL, use_reader_first=True)
        print(f"[DEBUG] Aussie BB response length: {len(abb_html)} chars")
        abb_items = parse_aussie_bb_status(abb_html)
        print(f"[INFO] Parsed {len(abb_items)} active/upcoming Aussie Broadband records.")
        all_records.extend(abb_items)
    except Exception as exc:
        print(f"[WARN] Could not parse Aussie Broadband status page: {exc}")

    # 4. Fetch Launtel Live Outages & Announcements Feed
    try:
        print(f"[INFO] Fetching Launtel Status: {LAUNTEL_OUTAGES_URL}")
        lau_html = fetch_url_text(LAUNTEL_OUTAGES_URL, use_reader_first=True)
        print(f"[DEBUG] Launtel response length: {len(lau_html)} chars")
        lau_items = parse_launtel_status(lau_html)
        print(f"[INFO] Parsed {len(lau_items)} active Launtel records.")
        all_records.extend(lau_items)
    except Exception as exc:
        print(f"[WARN] Could not parse Launtel status page: {exc}")

    # 5. Fetch unblocked Australian RSP Statuspage JSON APIs (Superloop, Swoop, Exetel)
    for rsp_name, sp_url in STATUSPAGE_ENDPOINTS:
        try:
            print(f"[INFO] Fetching {rsp_name} Status API: {sp_url}")
            sp_json = fetch_url_text(sp_url)
            sp_items = parse_statuspage_summary(rsp_name, sp_json)
            print(f"[INFO] Parsed {len(sp_items)} active {rsp_name} records.")
            all_records.extend(sp_items)
        except Exception as exc:
            print(f"[WARN] Could not parse {rsp_name} status API: {exc}")

    # 6. Fetch any optional custom RSP JSON feeds
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
