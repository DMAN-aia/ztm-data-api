"""
Understat endpoints — eigen scraper (JSON embedded in HTML)
"""

import re
import json
import codecs
import requests
from fastapi import APIRouter, HTTPException, Query
from app.utils.common import ok, cache_key, cache_get, cache_set

router = APIRouter()

TTL = 21600

UNDERSTAT_LEAGUES = {
    "GB1": "EPL",
    "L1":  "Bundesliga",
    "IT1": "Serie_A",
    "FR1": "Ligue_1",
    "ES1": "La_Liga",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://understat.com/",
}

def format_season(s: str) -> str:
    if len(s) == 4:
        return "20" + s[:2]
    return s

def us_get(url: str) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=20)
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Understat returned {resp.status_code}")
    return resp.text

def extract_json(html: str, var_name: str):
    """Extract JSON from Understat's embedded script vars."""
    pattern = rf"var\s+{var_name}\s*=\s*JSON\.parse\('(.+?)'\)\s*;"
    match = re.search(pattern, html, re.DOTALL)
    if not match:
        # Try alternate pattern without semicolon
        pattern2 = rf"var\s+{var_name}\s*=\s*JSON\.parse\('(.+?)'\)"
        match = re.search(pattern2, html, re.DOTALL)
    if not match:
        raise HTTPException(status_code=502, detail=f"Variable '{var_name}' not found in Understat page")
    raw = match.group(1)
    # Understat escapes with \x and \u — decode properly
    raw = raw.replace("\\'", "'")
    try:
        # First try: direct JSON parse after basic unescape
        unescaped = raw.encode('utf-8').decode('unicode_escape').encode('latin-1').decode('utf-8')
        return json.loads(unescaped)
    except Exception:
        try:
            # Second try: codecs
            unescaped = codecs.decode(raw.replace('\\x', '\\u00'), 'unicode_escape')
            return json.loads(unescaped)
        except Exception:
            try:
                # Third try: regex unescape \xNN
                def replace_hex(m):
                    return chr(int(m.group(1), 16))
                cleaned = re.sub(r'\\x([0-9a-fA-F]{2})', replace_hex, raw)
                return json.loads(cleaned)
            except Exception as e:
                raise HTTPException(status_code=502, detail=f"Failed to decode {var_name}: {e}")


@router.get("/player/season/{league_id}")
def player_season_stats(league_id: str, season: str = Query("2425")):
    lid = league_id.upper()
    league = UNDERSTAT_LEAGUES.get(lid)
    if not league:
        raise HTTPException(status_code=400, detail=f"Supported: {', '.join(UNDERSTAT_LEAGUES)}")

    ck = cache_key("understat", "player_season", league=lid, season=season)
    cached = cache_get(ck, TTL)
    if cached:
        return ok(cached, "understat", cached=True)

    url = f"https://understat.com/league/{league}/{format_season(season)}"
    try:
        html = us_get(url)
        data = extract_json(html, "playersData")
        cache_set(ck, data)
        return ok(data, "understat")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/team/season/{league_id}")
def team_season_stats(league_id: str, season: str = Query("2425")):
    lid = league_id.upper()
    league = UNDERSTAT_LEAGUES.get(lid)
    if not league:
        raise HTTPException(status_code=400, detail=f"Supported: {', '.join(UNDERSTAT_LEAGUES)}")

    ck = cache_key("understat", "team_season", league=lid, season=season)
    cached = cache_get(ck, TTL)
    if cached:
        return ok(cached, "understat", cached=True)

    url = f"https://understat.com/league/{league}/{format_season(season)}"
    try:
        html = us_get(url)
        data = extract_json(html, "teamsData")
        flat = [{"team": k, **v} for k, v in data.items()] if isinstance(data, dict) else data
        cache_set(ck, flat)
        return ok(flat, "understat")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/shots/{league_id}")
def shots(league_id: str, season: str = Query("2425")):
    lid = league_id.upper()
    league = UNDERSTAT_LEAGUES.get(lid)
    if not league:
        raise HTTPException(status_code=400, detail=f"Supported: {', '.join(UNDERSTAT_LEAGUES)}")

    ck = cache_key("understat", "shots", league=lid, season=season)
    cached = cache_get(ck, TTL)
    if cached:
        return ok(cached, "understat", cached=True)

    url = f"https://understat.com/league/{league}/{format_season(season)}"
    try:
        html = us_get(url)
        data = extract_json(html, "shotsData")
        cache_set(ck, data)
        return ok(data, "understat")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
