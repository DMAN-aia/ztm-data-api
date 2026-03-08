"""
Understat endpoints — eigen scraper (JSON embedded in HTML)
Understat.com embeds alle data als JSON in script tags — geen Selenium nodig.

Supported leagues: GB1, L1, IT1, FR1, ES1
Endpoints:
  GET /understat/player/season/{league_id}
  GET /understat/team/season/{league_id}
  GET /understat/shots/{league_id}
"""

import re
import json
import requests
from urllib.parse import unquote
from fastapi import APIRouter, HTTPException, Query
from app.utils.common import ok, cache_key, cache_get, cache_set

router = APIRouter()

TTL = 21600  # 6 hours

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
    """'2425' → '2024'  (Understat uses start year only)"""
    if len(s) == 4:
        return "20" + s[:2]
    return s

def us_get(url: str) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=20)
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Understat returned {resp.status_code}")
    return resp.text

def extract_json(html: str, var_name: str) -> list | dict:
    """Extract JSON from Understat's embedded script: var name = JSON.parse('...')"""
    pattern = rf"var\s+{var_name}\s*=\s*JSON\.parse\('(.+?)'\)"
    match = re.search(pattern, html)
    if not match:
        raise HTTPException(status_code=502, detail=f"Variable '{var_name}' not found in Understat page")
    # Understat encodes the JSON string — unescape it
    raw = match.group(1).encode().decode("unicode_escape")
    return json.loads(raw)


# ─────────────────────────────────────────
# PLAYER SEASON xG
# ─────────────────────────────────────────

@router.get("/player/season/{league_id}")
def player_season_stats(
    league_id: str,
    season: str = Query("2425"),
):
    lid = league_id.upper()
    league = UNDERSTAT_LEAGUES.get(lid)
    if not league:
        raise HTTPException(status_code=400, detail=f"Understat supports: {', '.join(UNDERSTAT_LEAGUES)}")

    ck = cache_key("understat", "player_season", league=lid, season=season)
    cached = cache_get(ck, TTL)
    if cached:
        return ok(cached, "understat", cached=True)

    season_year = format_season(season)
    url = f"https://understat.com/league/{league}/{season_year}"

    try:
        html = us_get(url)
        data = extract_json(html, "playersData")
        cache_set(ck, data)
        return ok(data, "understat")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────
# TEAM SEASON xG
# ─────────────────────────────────────────

@router.get("/team/season/{league_id}")
def team_season_stats(
    league_id: str,
    season: str = Query("2425"),
):
    lid = league_id.upper()
    league = UNDERSTAT_LEAGUES.get(lid)
    if not league:
        raise HTTPException(status_code=400, detail=f"Understat supports: {', '.join(UNDERSTAT_LEAGUES)}")

    ck = cache_key("understat", "team_season", league=lid, season=season)
    cached = cache_get(ck, TTL)
    if cached:
        return ok(cached, "understat", cached=True)

    season_year = format_season(season)
    url = f"https://understat.com/league/{league}/{season_year}"

    try:
        html = us_get(url)
        data = extract_json(html, "teamsData")
        # teamsData is a dict keyed by team name — flatten to list
        flat = [{"team": k, **v} for k, v in data.items()]
        cache_set(ck, flat)
        return ok(flat, "understat")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────
# SHOT-LEVEL xG
# ─────────────────────────────────────────

@router.get("/shots/{league_id}")
def shots(
    league_id: str,
    season: str = Query("2425"),
):
    lid = league_id.upper()
    league = UNDERSTAT_LEAGUES.get(lid)
    if not league:
        raise HTTPException(status_code=400, detail=f"Understat supports: {', '.join(UNDERSTAT_LEAGUES)}")

    ck = cache_key("understat", "shots", league=lid, season=season)
    cached = cache_get(ck, TTL)
    if cached:
        return ok(cached, "understat", cached=True)

    season_year = format_season(season)
    url = f"https://understat.com/league/{league}/{season_year}"

    try:
        html = us_get(url)
        data = extract_json(html, "shotsData")
        cache_set(ck, data)
        return ok(data, "understat")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
