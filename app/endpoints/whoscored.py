"""
WhoScored endpoints — via soccerdata (Selenium headless)
Data: match events, player ratings, heatmaps

Available endpoints:
  GET /whoscored/events/{league_id}    — match events (shots, passes, fouls, cards)
  GET /whoscored/schedule/{league_id}  — fixtures + results with ratings

Note: WhoScored requires Selenium/Chromium. Requests are slower (~5-10s per match).
Use cache aggressively — TTL 6 hours for completed matches.
"""

import soccerdata as sd
from fastapi import APIRouter, HTTPException, Query
from app.utils.common import (
    SOCCERDATA_LEAGUES, ok, cache_key, cache_get, cache_set, resolve_league
)

router = APIRouter()

TTL_EVENTS   = 21600   # 6 hours — match events don't change after FT
TTL_SCHEDULE = 3600    # 1 hour


def get_whoscored(league_id: str, season: str):
    league = resolve_league(league_id, SOCCERDATA_LEAGUES)
    if not league:
        raise HTTPException(status_code=400, detail=f"Unsupported league: {league_id}")
    return sd.WhoScored(leagues=league, seasons=season, no_cache=True)


# ─────────────────────────────────────────
# MATCH EVENTS
# ─────────────────────────────────────────

@router.get("/events/{league_id}")
def match_events(
    league_id: str,
    season: str   = Query("2425"),
    match_id: int = Query(None, description="Optional: single WhoScored match ID"),
):
    """
    Full event stream per match: shots, passes, tackles, fouls, cards, subs.
    Each event has: minute, type, player, team, outcome, x/y coordinates.
    """
    ck = cache_key("whoscored", "events", league=league_id, season=season, mid=match_id)
    cached = cache_get(ck, TTL_EVENTS)
    if cached:
        return ok(cached, "whoscored", cached=True)

    try:
        ws = get_whoscored(league_id, season)
        if match_id:
            df = ws.read_events(match_id=[match_id])
        else:
            df = ws.read_events()
        data = df.reset_index().to_dict(orient="records")
        cache_set(ck, data)
        return ok(data, "whoscored")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────
# SCHEDULE + RATINGS
# ─────────────────────────────────────────

@router.get("/schedule/{league_id}")
def schedule(
    league_id: str,
    season: str = Query("2425"),
):
    """Fixtures and results with WhoScored match ratings."""
    ck = cache_key("whoscored", "schedule", league=league_id, season=season)
    cached = cache_get(ck, TTL_SCHEDULE)
    if cached:
        return ok(cached, "whoscored", cached=True)

    try:
        ws = get_whoscored(league_id, season)
        df = ws.read_schedule()
        data = df.reset_index().to_dict(orient="records")
        cache_set(ck, data)
        return ok(data, "whoscored")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
