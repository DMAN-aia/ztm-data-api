"""
Sofascore endpoints — via soccerdata (unofficial JSON API)
Data: schedules, results, live scores, H2H, lineups, form

Available endpoints:
  GET /sofascore/schedule/{league_id}   — fixtures + results
  GET /sofascore/standings/{league_id}  — league table
"""

import soccerdata as sd
from fastapi import APIRouter, HTTPException, Query
from app.utils.common import (
    SOCCERDATA_LEAGUES, ok, cache_key, cache_get, cache_set, resolve_league
)

router = APIRouter()

TTL_SCHEDULE  = 1800   # 30 min — scores update frequently
TTL_STANDINGS = 3600   # 1 hour


def get_sofascore(league_id: str, season: str):
    league = resolve_league(league_id, SOCCERDATA_LEAGUES)
    if not league:
        raise HTTPException(status_code=400, detail=f"Unsupported league: {league_id}")
    return sd.Sofascore(leagues=league, seasons=season, no_cache=True)


# ─────────────────────────────────────────
# SCHEDULE / RESULTS
# ─────────────────────────────────────────

@router.get("/schedule/{league_id}")
def schedule(
    league_id: str,
    season: str = Query("2425"),
):
    """Fixtures and results with scores."""
    ck = cache_key("sofascore", "schedule", league=league_id, season=season)
    cached = cache_get(ck, TTL_SCHEDULE)
    if cached:
        return ok(cached, "sofascore", cached=True)

    try:
        ss = get_sofascore(league_id, season)
        df = ss.read_schedule()
        data = df.reset_index().to_dict(orient="records")
        cache_set(ck, data)
        return ok(data, "sofascore")
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
    """League table / standings."""
    ck = cache_key("sofascore", "standings", league=league_id, season=season)
    cached = cache_get(ck, TTL_STANDINGS)
    if cached:
        return ok(cached, "sofascore", cached=True)

    try:
        ss = get_sofascore(league_id, season)
        df = ss.read_team_season_stats()
        data = df.reset_index().to_dict(orient="records")
        cache_set(ck, data)
        return ok(data, "sofascore")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
