#!/usr/bin/env python3
"""
===============================================================================
NBN & Opticomm Hourly Outage Collector (ACMA Register & RSP Feed Normalizer)
===============================================================================
Purpose:
  Fetches public ACMA Major/Significant Local Outage registers (CSV/JSON) for
  both NBN Co and Opticomm, normalizes locations (Suburb, Postcode, State),
  converts timestamps into plain-English 12-hour AM/PM state local time, and
  writes `outages.json` (for the web dashboard) + `outages_gem_knowledge.csv`
  (for Google Sheets / Gemini Gems).

Usage:
  1. Run once manually:
       python3 outage_collector.py
  2. Run continuously as a built-in hourly daemon (every 60 minutes):
       python3 outage_collector.py --loop
  3. Or schedule via GitHub Actions / cron / Windows Task Scheduler every hour.

Dependencies:
  Uses only Python 3.8+ standard library (`urllib`, `csv`, `json`, `re`,
  `datetime`, `zoneinfo`) — no external pip packages required!
===============================================================================
"""

import argparse
import csv
import io
import json
import logging
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

# -----------------------------------------------------------------------------
# 1. CONFIGURATION & ENDPOINTS
# -----------------------------------------------------------------------------
OUTPUT_JSON_PATH = os.getenv("OUTAGE_JSON_PATH", "outages.json")
OUTPUT_CSV_PATH = os.getenv("OUTAGE_CSV_PATH", "outages_gem_knowledge.csv")
POLL_INTERVAL_SECONDS = 3600  # 1 hour (60 minutes)

# Official public ACMA Outage Register pages & configurable direct CSV/JSON URLs.
# Note: NBN & Opticomm periodically rotate their direct CSV download links on
# their register pages; the scraper below automatically discovers the latest
# `.csv` link from the HTML page if a direct CSV URL isn't overridden via env vars.
SOURCES = {
    "nbn_register_page": os.getenv(
        "NBN_REGISTER_URL",
        "https://www.nbnco.com.au/utility/major-significant-local-outages-register"
    ),
    "nbn_direct_csv": os.getenv("NBN_DIRECT_CSV_URL", ""),
    "opticomm_register_page": os.getenv(
        "OPTICOMM_REGISTER_URL",
        "https://www.opticomm.com.au/outages/register/"
    ),
    # Direct Opticomm ACMA Register CSV API endpoint
    "opticomm_direct_csv": os.getenv(
        "OPTICOMM_DIRECT_CSV_URL",
        "https://www.opticomm.com.au/api/network/register/?format=csv"
    ),
    # Optional: Add any public RSP status JSON feed URL here
    "optional_rsp_json": os.getenv("RSP_STATUS_JSON_URL", "")
}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "OutageMonitorBot/1.0 (+https://github.com/aus-noc-outage-monitor)"
)

# Australian State / Territory Timezone Mapping
STATE_TIMEZONES: Dict[str, Tuple[str, str]] = {
    "WA":  ("Australia/Perth", "AWST"),
    "SA":  ("Australia/Adelaide", "ACDT"),
    "NT":  ("Australia/Darwin", "ACST"),
    "QLD": ("Australia/Brisbane", "AEST"),
    "NSW": ("Australia/Sydney", "AEDT"),
    "VIC": ("Australia/Melbourne", "AEDT"),
    "TAS": ("Australia/Hobart", "AEDT"),
    "ACT": ("Australia/Canberra", "AEDT"),
}

# Postcode prefix fallback if State column is missing in a CSV row
POSTCODE_PREFIX_TO_STATE = {
    "0": "NT",
    "2": "NSW",  # Note: 2600-2618 & 2900-2920 refined to ACT below
    "3": "VIC",
    "4": "QLD",
    "5": "SA",
    "6": "WA",
    "7": "TAS",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%I:%M:%S %p"
)
logger = logging.getLogger("OutageCollector")


# -----------------------------------------------------------------------------
# 2. HTTP FETCHING & CSV LINK DISCOVERY HELPERS
# -----------------------------------------------------------------------------
def fetch_url_text(url: str, timeout: int = 25) -> str:
    """Fetches a URL using urllib with browser-safe headers and UTF-8 decoding."""
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/csv,application/json,text/html,*/*;q=0.8",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw_bytes = resp.read()
        # Handle UTF-8 with BOM (common in Excel/ACMA CSV exports)
        return raw_bytes.decode("utf-8-sig", errors="replace")


def discover_csv_link(page_url: str) -> Optional[str]:
    """
    Scrapes an ACMA register webpage (NBN or Opticomm) to find the latest
    downloadable `.csv` file URL.
    """
    try:
        html = fetch_url_text(page_url)
        # Look for href="...something.csv..."
        matches = re.findall(r'href=["\']([^"\']+\.csv(?:\?[^"\']*)?)["\']', html, re.IGNORECASE)
        if not matches:
            return None

        csv_href = matches[0]
        if csv_href.startswith("http"):
            return csv_href
        # Resolve relative URL
        from urllib.parse import urljoin
        return urljoin(page_url, csv_href)
    except Exception as exc:
        logger.warning("Could not discover CSV link from %s: %s", page_url, exc)
        return None


# -----------------------------------------------------------------------------
# 3. LOCATION & 12-HOUR AM/PM TIME NORMALIZATION HELPERS
# -----------------------------------------------------------------------------
def infer_state(raw_state: str, postcodes: List[str]) -> str:
    """Normalizes state strings ('Western Australia' -> 'WA') or infers from postcode."""
    cleaned = (raw_state or "").strip().upper()
    name_map = {
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
    if cleaned in name_map:
        return name_map[cleaned]

    # Infer from first valid 4-digit postcode
    for pc in postcodes:
        digits = re.sub(r"\D", "", pc)
        if len(digits) == 4:
            pc_num = int(digits)
            if (2600 <= pc_num <= 2618) or (2900 <= pc_num <= 2920):
                return "ACT"
            return POSTCODE_PREFIX_TO_STATE.get(digits[0], "NSW")
    return "NSW"


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

    # Try ISO 8601 parsing (e.g., 2026-11-21T17:00:00Z or 2026-11-22 01:00:00+08:00)
    iso_candidate = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso_candidate)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=target_tz)
        else:
            dt = dt.astimezone(target_tz)
        hour_12 = dt.strftime("%I").lstrip("0") or "12"
        return f"{dt.strftime('%Y-%m-%d')} {hour_12}:{dt.strftime('%M %p')} {tz_abbr}"
    except ValueError:
        pass

    # Try common Australian CSV formats (DD/MM/YYYY HH:MM or YYYY-MM-DD HH:MM)
    patterns = [
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%d-%m-%Y %H:%M",
    ]
    # Strip trailing timezone letters before strptime
    stripped_tz = re.sub(r"\s+[A-Z]{3,4}$", "", text)
    for fmt in patterns:
        try:
            dt = datetime.strptime(stripped_tz, fmt).replace(tzinfo=target_tz)
            hour_12 = dt.strftime("%I").lstrip("0") or "12"
            return f"{dt.strftime('%Y-%m-%d')} {hour_12}:{dt.strftime('%M %p')} {tz_abbr}"
        except ValueError:
            continue

    # Return cleaned original if already human-readable (e.g., "Restored 1:40 PM ACDT")
    return text


def normalize_status(raw_status: str) -> str:
    """Maps carrier-specific status labels to the 4 dashboard statuses."""
    s = (raw_status or "").lower()
    if any(k in s for k in ("resolved", "restored", "closed", "completed")):
        return "Resolved"
    if any(k in s for k in ("planned", "scheduled", "maintenance", "upgrade")):
        return "Planned Maintenance"
    if any(k in s for k in ("degrad", "intermittent", "partial", "congestion")):
        return "Degraded Performance"
    return "Unplanned Outage"


def split_list_field(raw_val: str) -> List[str]:
    """Splits comma/semicolon/pipe-separated suburbs or postcodes cleanly."""
    if not raw_val:
        return []
    parts = re.split(r"[,;|/]+", raw_val)
    return [p.strip() for p in parts if p.strip()]


# -----------------------------------------------------------------------------
# 4. CSV PARSER FOR ACMA OUTAGE REGISTERS (NBN & OPTICOMM)
# -----------------------------------------------------------------------------
def parse_acma_csv(csv_text: str, carrier_name: str) -> List[Dict[str, Any]]:
    """
    Parses an ACMA Major/Significant Local Outages CSV into normalized
    dashboard incident objects. Handles varying column header names gracefully.
    """
    reader = csv.DictReader(io.StringIO(csv_text))
    if not reader.fieldnames:
        return []

    incidents: List[Dict[str, Any]] = []
    now_str = datetime.now(timezone.utc).astimezone(ZoneInfo("Australia/Perth")).strftime("%I:%M %p AWST").lstrip("0")

    for idx, row in enumerate(reader, start=1):
        # Case-insensitive column lookup helper
        lower_row = {(k or "").strip().lower(): (v or "").strip() for k, v in row.items()}

        def pick(*candidate_keys: str) -> str:
            for key in candidate_keys:
                for col_name, val in lower_row.items():
                    if key in col_name and val:
                        return val
            return ""

        raw_id = pick("reference", "incident", "outage id", "ticket", "id") or f"{carrier_name.upper()}-{idx:04d}"
        raw_title = pick("outage title", "title")
        raw_suburbs = pick("suburb", "town", "locality", "affected locations", "location", "area")
        raw_postcodes = pick("postcode", "post code", "affected postcodes", "zip")
        raw_state = pick("state", "territory", "affected states", "jurisdiction")
        raw_tech = pick("technology", "service type", "network", "access") or (
            "Opticomm Fibre" if carrier_name == "Opticomm" else "NBN Multi-Tech"
        )
        raw_rectified = pick("rectified", "date last rectified", "resolved date")
        raw_status = pick("status", "state of outage", "outage type", "type")
        raw_cause = pick("cause", "reason", "description", "details") or raw_title or "Carrier network fault under investigation."
        raw_start = pick("start", "commenced", "detected", "created", "date")
        raw_etr = raw_rectified or pick("restoration", "etr", "estimated", "end")
        raw_poi = pick("poi", "exchange", "headend", "node", "csa") or (raw_title if raw_title else "ACMA Register Feed")
        raw_premises = pick("premises", "services", "customers", "impacted", "affected services")

        suburbs = split_list_field(raw_suburbs) or ["Suburb Details Pending"]
        postcodes = split_list_field(raw_postcodes) or ["0000"]
        state_code = infer_state(raw_state, postcodes)

        # Parse numeric premises count safely
        premises_digits = re.sub(r"\D", "", raw_premises)
        affected_premises = int(premises_digits) if premises_digits else 0

        status = "Resolved" if raw_rectified else normalize_status(raw_status)
        start_12h = format_12h_local(raw_start, state_code)
        etr_12h = (
            f"Restored {format_12h_local(raw_rectified, state_code)}"
            if raw_rectified
            else format_12h_local(raw_etr, state_code)
        )
        raw_status = pick("status", "state of outage", "type")
        raw_cause = pick("cause", "reason", "description", "details") or "Carrier network fault under investigation."
        raw_start = pick("start", "commenced", "detected", "created")
        raw_etr = pick("restoration", "etr", "estimated", "end")
        raw_poi = pick("poi", "exchange", "headend", "node", "csa") or "ACMA Register Feed"
        raw_premises = pick("premises", "services", "customers", "impacted", "affected")

        suburbs = split_list_field(raw_suburbs) or ["Suburb Details Pending"]
        postcodes = split_list_field(raw_postcodes) or ["0000"]
        state_code = infer_state(raw_state, postcodes)

        # Parse numeric premises count safely
        premises_digits = re.sub(r"\D", "", raw_premises)
        affected_premises = int(premises_digits) if premises_digits else 0

        status = normalize_status(raw_status)
        start_12h = format_12h_local(raw_start, state_code)
        etr_12h = format_12h_local(raw_etr, state_code)

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
            "updates": [
                {
                    "time": now_str,
                    "note": f"Synchronized from official {carrier_name} ACMA Outage Register."
                }
            ]
        })

    return incidents


# -----------------------------------------------------------------------------
# 5. MAIN COLLECTION PIPELINE & EXPORT (JSON + CSV FOR GEMINI GEM)
# -----------------------------------------------------------------------------
def collect_all_outages() -> List[Dict[str, Any]]:
    """Fetches and merges NBN and Opticomm outage records."""
    all_records: List[Dict[str, Any]] = []

    # 1. Fetch NBN Co ACMA Outage Register
    nbn_csv_url = SOURCES["nbn_direct_csv"] or discover_csv_link(SOURCES["nbn_register_page"])
    if nbn_csv_url:
        try:
            logger.info("Fetching NBN ACMA register CSV: %s", nbn_csv_url)
            nbn_csv = fetch_url_text(nbn_csv_url)
            nbn_items = parse_acma_csv(nbn_csv, "NBN")
            logger.info("Parsed %d NBN outage records.", len(nbn_items))
            all_records.extend(nbn_items)
        except Exception as exc:
            logger.error("Failed to fetch/parse NBN CSV: %s", exc)
    else:
        logger.warning("No NBN CSV URL discovered; set NBN_DIRECT_CSV_URL if page structure changed.")

    # 2. Fetch Opticomm ACMA Outage Register
    opt_csv_url = SOURCES["opticomm_direct_csv"] or discover_csv_link(SOURCES["opticomm_register_page"])
    if opt_csv_url:
        try:
            logger.info("Fetching Opticomm ACMA register CSV: %s", opt_csv_url)
            opt_csv = fetch_url_text(opt_csv_url)
            opt_items = parse_acma_csv(opt_csv, "Opticomm")
            logger.info("Parsed %d Opticomm outage records.", len(opt_items))
            all_records.extend(opt_items)
        except Exception as exc:
            logger.error("Failed to fetch/parse Opticomm CSV: %s", exc)
    else:
        logger.warning("No Opticomm CSV URL discovered; set OPTICOMM_DIRECT_CSV_URL if needed.")

    # 3. Optional RSP JSON Status Feed
    if SOURCES["optional_rsp_json"]:
        try:
            logger.info("Fetching optional RSP status JSON: %s", SOURCES["optional_rsp_json"])
            rsp_raw = fetch_url_text(SOURCES["optional_rsp_json"])
            rsp_data = json.loads(rsp_raw)
            if isinstance(rsp_data, list):
                all_records.extend(rsp_data)
        except Exception as exc:
            logger.error("Optional RSP JSON fetch failed: %s", exc)

    # Deduplicate by Incident ID while preserving latest record
    deduped: Dict[str, Dict[str, Any]] = {}
    for rec in all_records:
        deduped[rec["id"]] = rec

    return list(deduped.values())


def export_outputs(records: List[Dict[str, Any]]) -> None:
    """
    Writes:
      1. `outages.json` -> Consumed directly by the HTML Live Outage Monitor.
      2. `outages_gem_knowledge.csv` -> Upload or sync to Google Sheets for your Gem.
    """
    # Write JSON array expected by the web dashboard
    with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as jf:
        json.dump(records, jf, indent=2, ensure_ascii=False)
    logger.info("Wrote %d records to %s", len(records), OUTPUT_JSON_PATH)

    # Write flat CSV optimized for Google Sheets / Gemini Gem natural-language queries
    fieldnames = [
        "Carrier",
        "Incident_ID",
        "Status",
        "State",
        "Suburbs",
        "Postcodes",
        "Technology",
        "POI_or_Exchange",
        "Start_Time_12h",
        "Estimated_Restoration_12h",
        "Affected_Premises",
        "Cause",
        "Summary",
    ]
    with open(OUTPUT_CSV_PATH, "w", newline="", encoding="utf-8") as cf:
        writer = csv.DictWriter(cf, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            writer.writerow({
                "Carrier": r.get("carrier", ""),
                "Incident_ID": r.get("id", ""),
                "Status": r.get("status", ""),
                "State": r.get("state", ""),
                "Suburbs": ", ".join(r.get("suburbs", [])),
                "Postcodes": ", ".join(r.get("postcodes", [])),
                "Technology": r.get("technology", ""),
                "POI_or_Exchange": r.get("poiOrExchange", ""),
                "Start_Time_12h": r.get("startTime", ""),
                "Estimated_Restoration_12h": r.get("etr", ""),
                "Affected_Premises": r.get("affectedPremises", 0),
                "Cause": r.get("cause", ""),
                "Summary": r.get("summary", ""),
            })
    logger.info("Wrote Gem-ready CSV table to %s", OUTPUT_CSV_PATH)


def run_once() -> None:
    logger.info("Starting NBN & Opticomm outage collection cycle...")
    records = collect_all_outages()
    if not records:
        logger.warning(
            "0 live records retrieved (carriers may currently have zero ACMA-listed major outages, "
            "or a register page requires a direct CSV URL). Writing empty valid JSON array."
        )
    export_outputs(records)


def main() -> None:
    parser = argparse.ArgumentParser(description="Hourly NBN & Opticomm Outage Collector")
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Keep running continuously and poll every 60 minutes."
    )
    args = parser.parse_args()

    if args.loop:
        logger.info("Running in continuous hourly daemon mode (interval: %ds)...", POLL_INTERVAL_SECONDS)
        while True:
            try:
                run_once()
            except Exception as exc:
                logger.exception("Unhandled error during hourly sync: %s", exc)
            logger.info("Sleeping 60 minutes until next hourly check...")
            time.sleep(POLL_INTERVAL_SECONDS)
    else:
        run_once()


if __name__ == "__main__":
    main()
