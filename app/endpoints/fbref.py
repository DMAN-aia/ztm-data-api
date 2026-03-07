"""
FBref endpoints — via soccerdata
All data sourced from fbref.com (StatHead/Sports Reference)

Available endpoints:
  GET /fbref/schedule/{league_id}
  GET /fbref/player/season/{league_id}
  GET /fbref/team/season/{league_id}
  GET /fbref/player/match/{league_id}
  GET /fbref/team/match/{league_id}

league_id examples: GB1, L1, IT1, FR1, NL1, ES1, CL
stat_type options: standard, shooting, passing, defense, possession, misc, keeper
"""

import soccerdata as sd
from fastapi import APIRouter, HTTPException, Query
from app.utils.common import (
    SOCCERDATA_LEAGUES, ok, err, cache_key, cache_get, cache_set, resolve_league
)

router = APIRouter()

# Cache TTLs
TTL_SCHEDULE   = 3600      # 1 hour
TTL_STATS      = 21600     # 6 hours
TTL_LIVE       = 300       # 5 minutes (current season schedule)


def get_fbref(league_id: str, season: str):
    league = resolve_league(league_id, SOCCERDATA_LEAGUES)
    if not league:
        raise HTTPException(status_code=400, detail=f"Unsupported league: {league_id}")
    return sd.FBref(leagues=league, seasons=season, no_cache=True)


# ─────────────────────────────────────────
# SCHEDULE / FIXTURES
# ─────────────────────────────────────────

@router.get("/schedule/{league_id}")
def schedule(
    league_id: str,
    season: str = Query("2425", description="Season code, e.g. 2425 for 2024/25"),
):
    """Match schedule + results for a league/season."""
    ck = cache_key("fbref", "schedule", league=league_id, season=season)
    cached = cache_get(ck, TTL_SCHEDULE)
    if cached:
        return ok(cached, "fbref", cached=True)

    try:
        fbref = get_fbref(league_id, season)
        df = fbref.read_schedule()
        data = df.reset_index().to_dict(orient="records")
        cache_set(ck, data)
        return ok(data, "fbref")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────
# PLAYER SEASON STATS
# ─────────────────────────────────────────

@router.get("/player/season/{league_id}")
def player_season_stats(
    league_id: str,
    season: str    = Query("2425"),
    stat_type: str = Query("standard", description="standard|shooting|passing|defense|possession|misc|keeper"),
):
    """Aggregated player stats for the season."""
    ck = cache_key("fbref", "player_season", league=league_id, season=season, stat=stat_type)
    cached = cache_get(ck, TTL_STATS)
    if cached:
        return ok(cached, "fbref", cached=True)

    try:
        fbref = get_fbref(league_id, season)
        df = fbref.read_player_season_stats(stat_type=stat_type)
        data = df.reset_index().to_dict(orient="records")
        cache_set(ck, data)
        return ok(data, "fbref")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────
# TEAM SEASON STATS
# ─────────────────────────────────────────

@router.get("/team/season/{league_id}")
def team_season_stats(
    league_id: str,
    season: str    = Query("2425"),
    stat_type: str = Query("standard"),
):
    """Aggregated team stats for the season."""
    ck = cache_key("fbref", "team_season", league=league_id, season=season, stat=stat_type)
    cached = cache_get(ck, TTL_STATS)
    if cached:
        return ok(cached, "fbref", cached=True)

    try:
        fbref = get_fbref(league_id, season)
        df = fbref.read_team_season_stats(stat_type=stat_type)
        data = df.reset_index().to_dict(orient="records")
        cache_set(ck, data)
        return ok(data, "fbref")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────
# PLAYER MATCH STATS
# ─────────────────────────────────────────

@router.get("/player/match/{league_id}")
def player_match_stats(
    league_id: str,
    season: str    = Query("2425"),
    stat_type: str = Query("standard"),
    match_id: str  = Query(None, description="Optional: single FBref match ID"),
):
    """Per-match player stats. Optionally filter by match_id."""
    ck = cache_key("fbref", "player_match", league=league_id, season=season, stat=stat_type, mid=match_id)
    cached = cache_get(ck, TTL_STATS)
    if cached:
        return ok(cached, "fbref", cached=True)

    try:
        fbref = get_fbref(league_id, season)
        if match_id:
            df = fbref.read_player_match_stats(stat_type=stat_type, match_id=match_id)
        else:
            df = fbref.read_player_match_stats(stat_type=stat_type)
        data = df.reset_index().to_dict(orient="records")
        cache_set(ck, data)
        return ok(data, "fbref")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────
# TEAM MATCH STATS
# ─────────────────────────────────────────

@router.get("/team/match/{league_id}")
def team_match_stats(
    league_id: str,
    season: str    = Query("2425"),
    stat_type: str = Query("schedule", description="schedule|shooting|passing|defense|possession|misc|keeper"),
    team: str      = Query(None, description="Optional: filter by team name"),
):
    """Per-match team stats."""
    ck = cache_key("fbref", "team_match", league=league_id, season=season, stat=stat_type, team=team)
    cached = cache_get(ck, TTL_STATS)
    if cached:
        return ok(cached, "fbref", cached=True)

    try:
        fbref = get_fbref(league_id, season)
        kwargs = {"stat_type": stat_type}
        if team:
            kwargs["team"] = team
        df = fbref.read_team_match_stats(**kwargs)
        data = df.reset_index().to_dict(orient="records")
        cache_set(ck, data)
        return ok(data, "fbref")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
