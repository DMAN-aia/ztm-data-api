"""
Understat endpoints — via soccerdata
Data: xG per shot, player xG/xA season, team xG breakdown

Supported leagues: GB1, L1, IT1, FR1, ES1 (Understat limitation)

Available endpoints:
  GET /understat/player/season/{league_id}  — player xG/xA for the season
  GET /understat/player/match/{league_id}   — player xG per match
  GET /understat/team/season/{league_id}    — team xG summary
  GET /understat/shots/{league_id}          — all shots with xG values
"""

import soccerdata as sd
from fastapi import APIRouter, HTTPException, Query
from app.utils.common import (
    UNDERSTAT_LEAGUES, ok, cache_key, cache_get, cache_set, resolve_league
)

router = APIRouter()

TTL = 21600   # 6 hours


def get_understat(league_id: str, season: str):
    league = resolve_league(league_id, UNDERSTAT_LEAGUES)
    if not league:
        raise HTTPException(
            status_code=400,
            detail=f"Understat does not support league: {league_id}. Supported: GB1, L1, IT1, FR1, ES1"
        )
    return sd.Understat(leagues=league, seasons=season, no_cache=True)


# ─────────────────────────────────────────
# PLAYER SEASON xG
# ─────────────────────────────────────────

@router.get("/player/season/{league_id}")
def player_season_stats(
    league_id: str,
    season: str = Query("2425"),
):
    """Player xG, xA, npxG, key passes for the season."""
    ck = cache_key("understat", "player_season", league=league_id, season=season)
    cached = cache_get(ck, TTL)
    if cached:
        return ok(cached, "understat", cached=True)

    try:
        us = get_understat(league_id, season)
        df = us.read_player_season_stats()
        data = df.reset_index().to_dict(orient="records")
        cache_set(ck, data)
        return ok(data, "understat")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────
# PLAYER MATCH xG
# ─────────────────────────────────────────

@router.get("/player/match/{league_id}")
def player_match_stats(
    league_id: str,
    season: str = Query("2425"),
):
    """Player xG/xA per match."""
    ck = cache_key("understat", "player_match", league=league_id, season=season)
    cached = cache_get(ck, TTL)
    if cached:
        return ok(cached, "understat", cached=True)

    try:
        us = get_understat(league_id, season)
        df = us.read_player_match_stats()
        data = df.reset_index().to_dict(orient="records")
        cache_set(ck, data)
        return ok(data, "understat")
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
    """Team xG, xGA, npxG for the season."""
    ck = cache_key("understat", "team_season", league=league_id, season=season)
    cached = cache_get(ck, TTL)
    if cached:
        return ok(cached, "understat", cached=True)

    try:
        us = get_understat(league_id, season)
        df = us.read_team_season_stats()
        data = df.reset_index().to_dict(orient="records")
        cache_set(ck, data)
        return ok(data, "understat")
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
    """Every shot with xG value, x/y coordinates, player, situation."""
    ck = cache_key("understat", "shots", league=league_id, season=season)
    cached = cache_get(ck, TTL)
    if cached:
        return ok(cached, "understat", cached=True)

    try:
        us = get_understat(league_id, season)
        df = us.read_shots()
        data = df.reset_index().to_dict(orient="records")
        cache_set(ck, data)
        return ok(data, "understat")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
