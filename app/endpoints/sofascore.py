"""
Sofascore endpoints — eigen scraper (unofficiële JSON API)
Season IDs worden dynamisch opgehaald via de Sofascore API.
"""

import requests
from fastapi import APIRouter, HTTPException, Query
from app.utils.common import ok, cache_key, cache_get, cache_set

router = APIRouter()

TTL_SCHEDULE  = 1800
TTL_STANDINGS = 3600
TTL_SEASON_ID = 86400  # season ID cache 24 uur

SOFASCORE_TOURNAMENTS = {
    "GB1":  17,
    "GB2":  18,
    "L1":   35,
    "IT1":  23,
    "FR1":  34,
    "NL1":  37,
    "ES1":  8,
    "CL":   7,
    "EL":   679,
    "MLS":  242,
    "JP1":  196,
    "KR1":  55,
    "TH1":  107,
    "VN1":  390,
    "MY1":  672,
    "SA":   955,
    "AL":   180,
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.sofascore.com/",
    "Origin": "https://www.sofascore.com",
    "Cache-Control": "no-cache",
}

BASE = "https://api.sofascore.com/api/v1"

def ss_get(path: str) -> dict:
    resp = requests.get(f"{BASE}{path}", headers=HEADERS, timeout=15)
    if resp.status_code == 403:
        raise HTTPException(status_code=403, detail="Sofascore blocked request — IP may be restricted")
    if resp.status_code == 404:
        raise HTTPException(status_code=404, detail="Sofascore: resource not found")
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Sofascore returned {resp.status_code}")
    return resp.json()

def get_season_id(tournament_id: int, season_code: str) -> int:
    """Fetch current/recent season ID from Sofascore dynamically."""
    ck = cache_key("sofascore", "season_id", tid=tournament_id, season=season_code)
    cached = cache_get(ck, TTL_SEASON_ID)
    if cached:
        return cached["id"]

    data = ss_get(f"/unique-tournament/{tournament_id}/seasons")
    seasons = data.get("seasons", [])
    if not seasons:
        raise HTTPException(status_code=502, detail="No seasons found from Sofascore")

    # Try to match season_code (e.g. "2425" → year 2024 or 2025)
    if len(season_code) == 4:
        y1 = int("20" + season_code[:2])
        y2 = int("20" + season_code[2:])
        for s in seasons:
            yr = s.get("year", "")
            if str(y1) in str(yr) or str(y2) in str(yr):
                cache_set(ck, {"id": s["id"]})
                return s["id"]

    # Fallback: most recent season
    season = seasons[0]
    cache_set(ck, {"id": season["id"]})
    return season["id"]


@router.get("/schedule/{league_id}")
def schedule(
    league_id: str,
    season: str = Query("2425"),
    page: int   = Query(0),
):
    lid = league_id.upper()
    if lid not in SOFASCORE_TOURNAMENTS:
        raise HTTPException(status_code=400, detail=f"Unsupported league: {league_id}")

    ck = cache_key("sofascore", "schedule", league=lid, season=season, page=page)
    cached = cache_get(ck, TTL_SCHEDULE)
    if cached:
        return ok(cached, "sofascore", cached=True)

    tournament_id = SOFASCORE_TOURNAMENTS[lid]
    season_id = get_season_id(tournament_id, season)

    try:
        data = ss_get(f"/unique-tournament/{tournament_id}/season/{season_id}/events/last/{page}")
        events = data.get("events", [])

        matches = []
        for e in events:
            home = e.get("homeTeam", {})
            away = e.get("awayTeam", {})
            sh   = e.get("homeScore", {})
            sa   = e.get("awayScore", {})
            matches.append({
                "match_id":        e.get("id"),
                "status":          e.get("status", {}).get("description"),
                "start_timestamp": e.get("startTimestamp"),
                "round":           e.get("roundInfo", {}).get("round"),
                "home_team":       home.get("name"),
                "home_team_id":    home.get("id"),
                "away_team":       away.get("name"),
                "away_team_id":    away.get("id"),
                "home_score":      sh.get("current"),
                "away_score":      sa.get("current"),
                "home_score_ht":   sh.get("period1"),
                "away_score_ht":   sa.get("period1"),
                "winner_code":     e.get("winnerCode"),
            })

        cache_set(ck, matches)
        return ok(matches, "sofascore")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/standings/{league_id}")
def standings(
    league_id: str,
    season: str = Query("2425"),
):
    lid = league_id.upper()
    if lid not in SOFASCORE_TOURNAMENTS:
        raise HTTPException(status_code=400, detail=f"Unsupported league: {league_id}")

    ck = cache_key("sofascore", "standings", league=lid, season=season)
    cached = cache_get(ck, TTL_STANDINGS)
    if cached:
        return ok(cached, "sofascore", cached=True)

    tournament_id = SOFASCORE_TOURNAMENTS[lid]
    season_id = get_season_id(tournament_id, season)

    try:
        data = ss_get(f"/unique-tournament/{tournament_id}/season/{season_id}/standings/total")
        rows = data.get("standings", [{}])[0].get("rows", [])

        table = []
        for row in rows:
            team = row.get("team", {})
            table.append({
                "position":        row.get("position"),
                "team":            team.get("name"),
                "team_id":         team.get("id"),
                "played":          row.get("matches"),
                "won":             row.get("wins"),
                "drawn":           row.get("draws"),
                "lost":            row.get("losses"),
                "goals_for":       row.get("scoresFor"),
                "goals_against":   row.get("scoresAgainst"),
                "goal_difference": row.get("goalDifference"),
                "points":          row.get("points"),
            })

        cache_set(ck, table)
        return ok(table, "sofascore")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
