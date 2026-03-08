"""
Sofascore endpoints — eigen scraper (unofficiële JSON API)
Geen soccerdata.

Endpoints:
  GET /sofascore/schedule/{league_id}
  GET /sofascore/standings/{league_id}
"""

import requests
from fastapi import APIRouter, HTTPException, Query
from app.utils.common import ok, cache_key, cache_get, cache_set

router = APIRouter()

TTL_SCHEDULE  = 1800
TTL_STANDINGS = 3600

# Sofascore tournament IDs
SOFASCORE_TOURNAMENTS = {
    "GB1":  (17,   "Premier League"),
    "GB2":  (18,   "Championship"),
    "L1":   (35,   "Bundesliga"),
    "IT1":  (23,   "Serie A"),
    "FR1":  (34,   "Ligue 1"),
    "NL1":  (37,   "Eredivisie"),
    "ES1":  (8,    "La Liga"),
    "CL":   (7,    "UEFA Champions League"),
    "EL":   (679,  "UEFA Europa League"),
    "MLS":  (242,  "MLS"),
    "JP1":  (196,  "J1 League"),
    "KR1":  (55,   "K League 1"),
    "TH1":  (107,  "Thai League 1"),
    "VN1":  (390,  "V.League 1"),
    "MY1":  (672,  "Super League"),
    "SA":   (955,  "Saudi Pro League"),
    "AL":   (180,  "A-League Men"),
}

# Season IDs per tournament for 2024/25
# Sofascore uses numeric season IDs — these are for 2024/25
SOFASCORE_SEASONS = {
    "GB1":  61627,
    "GB2":  61628,
    "L1":   63516,
    "IT1":  63515,
    "FR1":  63517,
    "NL1":  63518,
    "ES1":  63519,
    "CL":   61644,
    "EL":   61645,
    "MLS":  57317,
    "JP1":  58882,
    "KR1":  58527,
    "TH1":  58528,
    "VN1":  58529,
    "MY1":  58530,
    "SA":   63513,
    "AL":   58531,
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.sofascore.com/",
    "Origin": "https://www.sofascore.com",
}

BASE = "https://api.sofascore.com/api/v1"

def ss_get(path: str) -> dict:
    url = f"{BASE}{path}"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    if resp.status_code == 403:
        raise HTTPException(status_code=403, detail="Sofascore blocked request")
    if resp.status_code == 404:
        raise HTTPException(status_code=404, detail="Sofascore: not found")
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Sofascore returned {resp.status_code}")
    return resp.json()


# ─────────────────────────────────────────
# SCHEDULE / RESULTS
# ─────────────────────────────────────────

@router.get("/schedule/{league_id}")
def schedule(
    league_id: str,
    season: str = Query("2425"),
    page: int   = Query(0, description="Pagination page (0-based)"),
):
    lid = league_id.upper()
    if lid not in SOFASCORE_TOURNAMENTS:
        raise HTTPException(status_code=400, detail=f"Unsupported league: {league_id}")

    ck = cache_key("sofascore", "schedule", league=lid, season=season, page=page)
    cached = cache_get(ck, TTL_SCHEDULE)
    if cached:
        return ok(cached, "sofascore", cached=True)

    tournament_id, _ = SOFASCORE_TOURNAMENTS[lid]
    season_id = SOFASCORE_SEASONS.get(lid)
    if not season_id:
        raise HTTPException(status_code=400, detail=f"No season ID for {league_id}")

    try:
        data = ss_get(f"/unique-tournament/{tournament_id}/season/{season_id}/events/last/{page}")
        events = data.get("events", [])

        matches = []
        for e in events:
            home = e.get("homeTeam", {})
            away = e.get("awayTeam", {})
            score_home = e.get("homeScore", {})
            score_away = e.get("awayScore", {})
            matches.append({
                "match_id":        e.get("id"),
                "status":          e.get("status", {}).get("description"),
                "start_timestamp": e.get("startTimestamp"),
                "round":           e.get("roundInfo", {}).get("round"),
                "home_team":       home.get("name"),
                "home_team_id":    home.get("id"),
                "away_team":       away.get("name"),
                "away_team_id":    away.get("id"),
                "home_score":      score_home.get("current"),
                "away_score":      score_away.get("current"),
                "home_score_ht":   score_home.get("period1"),
                "away_score_ht":   score_away.get("period1"),
                "winner_code":     e.get("winnerCode"),  # 1=home, 2=away, 3=draw
            })

        cache_set(ck, matches)
        return ok(matches, "sofascore")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────
# STANDINGS
# ─────────────────────────────────────────

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

    tournament_id, _ = SOFASCORE_TOURNAMENTS[lid]
    season_id = SOFASCORE_SEASONS.get(lid)
    if not season_id:
        raise HTTPException(status_code=400, detail=f"No season ID for {league_id}")

    try:
        data = ss_get(f"/unique-tournament/{tournament_id}/season/{season_id}/standings/total")
        standings_raw = data.get("standings", [{}])[0].get("rows", [])

        table = []
        for row in standings_raw:
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
                "form":            row.get("promotion", {}).get("text"),
            })

        cache_set(ck, table)
        return ok(table, "sofascore")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
