"""
Shared utilities for ZTM Data API
"""

import os
import json
import hashlib
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Optional

# ─────────────────────────────────────────
# LEAGUE MAPS
# ─────────────────────────────────────────

# Legacy league map (retained for reference)
SOCCERDATA_LEAGUES = {
    "GB1":  "ENG-Premier League",
    "GB2":  "ENG-Championship",
    "L1":   "DEU-Bundesliga",
    "IT1":  "ITA-Serie A",
    "FR1":  "FRA-Ligue 1",
    "NL1":  "NLD-Eredivisie",
    "ES1":  "ESP-La Liga",
    "CL":   "INT-UEFA Champions League",
    "EL":   "INT-UEFA Europa League",
    "MLS":  "USA-Major League Soccer",
    "JP1":  "JPN-J1 League",
    "KR1":  "KOR-K League 1",
    "TH1":  "THA-Thai League 1",
    "VN1":  "VNM-V.League 1",
    "MY1":  "MYS-Super League",
    "SA":   "SAU-Saudi Pro League",
    "AL":   "AUS-A-League Men",
}

# Understat only supports these leagues
UNDERSTAT_LEAGUES = {
    "GB1": "EPL",
    "L1":  "Bundesliga",
    "IT1": "Serie_A",
    "FR1": "Ligue_1",
    "ES1": "La_Liga",
    "RU1": "RFPL",
}

# ─────────────────────────────────────────
# SIMPLE FILE CACHE
# Used because Render has no Redis; resets on dyno restart
# For persistent caching, implement wp-side caching via api_cache table
# ─────────────────────────────────────────

CACHE_DIR = Path(os.getenv("CACHE_DIR", "/tmp/ztm_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

def cache_key(source: str, endpoint: str, **params) -> str:
    raw = f"{source}:{endpoint}:{json.dumps(params, sort_keys=True)}"
    return hashlib.md5(raw.encode()).hexdigest()

def cache_get(key: str, ttl_seconds: int = 3600) -> Optional[Any]:
    path = CACHE_DIR / f"{key}.json"
    if not path.exists():
        return None
    age = time.time() - path.stat().st_mtime
    if age > ttl_seconds:
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None

def cache_set(key: str, data: Any) -> None:
    path = CACHE_DIR / f"{key}.json"
    try:
        path.write_text(json.dumps(data, default=str))
    except Exception:
        pass

# ─────────────────────────────────────────
# RESPONSE HELPERS
# ─────────────────────────────────────────

def ok(data: Any, source: str, cached: bool = False) -> dict:
    return {
        "status": "ok",
        "source": source,
        "cached": cached,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "data": data,
    }

def err(message: str, source: str, status_code: int = 500) -> dict:
    return {
        "status": "error",
        "source": source,
        "message": message,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }

# ─────────────────────────────────────────
# LEAGUE VALIDATION
# ─────────────────────────────────────────

def resolve_league(league_id: str, league_map: dict) -> Optional[str]:
    """Resolve our internal league code to a source-specific string."""
    return league_map.get(league_id.upper())
