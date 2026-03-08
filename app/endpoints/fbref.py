"""
FBref endpoints — eigen scraper (requests + BeautifulSoup/lxml)
Geen soccerdata.

Endpoints:
  GET /fbref/schedule/{league_id}
  GET /fbref/player/season/{league_id}
  GET /fbref/team/season/{league_id}
"""

import time
import requests
from bs4 import BeautifulSoup
from fastapi import APIRouter, HTTPException, Query
from app.utils.common import ok, cache_key, cache_get, cache_set

router = APIRouter()

TTL_SCHEDULE = 3600
TTL_STATS    = 21600

FBREF_LEAGUES = {
    "GB1": ("9",  "Premier-League"),
    "GB2": ("10", "Championship"),
    "L1":  ("20", "Bundesliga"),
    "IT1": ("11", "Serie-A"),
    "FR1": ("13", "Ligue-1"),
    "NL1": ("23", "Eredivisie"),
    "ES1": ("12", "La-Liga"),
    "CL":  ("8",  "Champions-League"),
    "EL":  ("19", "Europa-League"),
    "MLS": ("22", "Major-League-Soccer"),
    "JP1": ("25", "J1-League"),
    "KR1": ("55", "K-League-1"),
    "SA":  ("70", "Saudi-Pro-League"),
    "AL":  ("53", "A-League-Men"),
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://fbref.com/",
}

def format_season(s: str) -> str:
    """'2425' → '2024-2025'"""
    if len(s) == 4:
        return f"20{s[:2]}-20{s[2:]}"
    return s

def fbref_get(url: str) -> BeautifulSoup:
    time.sleep(4)  # FBref: max 20 req/min
    resp = requests.get(url, headers=HEADERS, timeout=30)
    if resp.status_code == 429:
        raise HTTPException(status_code=429, detail="FBref rate limit hit — try again in 60s")
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"FBref returned {resp.status_code}")
    return BeautifulSoup(resp.text, "lxml")

def parse_table(soup: BeautifulSoup, table_id: str) -> list:
    table = soup.find("table", {"id": table_id})
    if not table:
        return []
    # Headers from last thead row (FBref has multi-row headers)
    thead = table.find("thead")
    headers = []
    if thead:
        for th in thead.find_all("tr")[-1].find_all(["th", "td"]):
            headers.append(th.get("data-stat") or th.get_text(strip=True) or f"col_{len(headers)}")
    tbody = table.find("tbody")
    if not tbody:
        return []
    rows = []
    for tr in tbody.find_all("tr"):
        classes = tr.get("class", [])
        if "thead" in classes or "spacer" in classes or "partial_table" in classes:
            continue
        cells = tr.find_all(["td", "th"])
        if not cells:
            continue
        row = {}
        for cell in cells:
            key = cell.get("data-stat", "")
            if not key:
                continue
            val = cell.get_text(strip=True)
            # Strip footnotes
            for sup in cell.find_all("sup"):
                sup.decompose()
            val = cell.get_text(strip=True)
            try:
                val = int(val)
            except ValueError:
                try:
                    val = float(val)
                except ValueError:
                    pass
            row[key] = val
        if row:
            rows.append(row)
    return rows


# ─────────────────────────────────────────
# SCHEDULE
# ─────────────────────────────────────────

@router.get("/schedule/{league_id}")
def schedule(
    league_id: str,
    season: str = Query("2425"),
):
    lid = league_id.upper()
    if lid not in FBREF_LEAGUES:
        raise HTTPException(status_code=400, detail=f"Unsupported league: {league_id}")

    ck = cache_key("fbref", "schedule", league=lid, season=season)
    cached = cache_get(ck, TTL_SCHEDULE)
    if cached:
        return ok(cached, "fbref", cached=True)

    comp_id, comp_name = FBREF_LEAGUES[lid]
    season_str = format_season(season)

    # Try season-specific URL first, fall back to current season
    urls = [
        f"https://fbref.com/en/comps/{comp_id}/{season_str}/schedule/{season_str}-{comp_name}-Scores-and-Fixtures",
        f"https://fbref.com/en/comps/{comp_id}/schedule/{comp_name}-Scores-and-Fixtures",
    ]

    data = []
    for url in urls:
        try:
            soup = fbref_get(url)
            # FBref schedule table id varies — find any sched_ table
            table = soup.find("table", id=lambda x: x and x.startswith("sched_"))
            if table:
                data = parse_table(soup, table["id"])
                if data:
                    break
        except HTTPException:
            raise
        except Exception:
            continue

    if not data:
        raise HTTPException(status_code=502, detail="Could not parse FBref schedule")

    cache_set(ck, data)
    return ok(data, "fbref")


# ─────────────────────────────────────────
# PLAYER SEASON STATS
# ─────────────────────────────────────────

STAT_TABLE_IDS = {
    "standard":   "stats_standard",
    "shooting":   "stats_shooting",
    "passing":    "stats_passing",
    "defense":    "stats_defense",
    "possession": "stats_possession",
    "misc":       "stats_misc",
    "keeper":     "stats_keeper",
}

@router.get("/player/season/{league_id}")
def player_season_stats(
    league_id: str,
    season: str    = Query("2425"),
    stat_type: str = Query("standard"),
):
    lid = league_id.upper()
    if lid not in FBREF_LEAGUES:
        raise HTTPException(status_code=400, detail=f"Unsupported league: {league_id}")
    if stat_type not in STAT_TABLE_IDS:
        raise HTTPException(status_code=400, detail=f"Unknown stat_type: {stat_type}")

    ck = cache_key("fbref", "player_season", league=lid, season=season, stat=stat_type)
    cached = cache_get(ck, TTL_STATS)
    if cached:
        return ok(cached, "fbref", cached=True)

    comp_id, comp_name = FBREF_LEAGUES[lid]
    season_str = format_season(season)
    stat_slug = stat_type if stat_type != "standard" else "stats"
    url = f"https://fbref.com/en/comps/{comp_id}/{season_str}/{stat_slug}/{season_str}-{comp_name}-Stats"

    try:
        soup = fbref_get(url)
        table_id = STAT_TABLE_IDS[stat_type]
        data = parse_table(soup, table_id)
        if not data:
            raise HTTPException(status_code=502, detail=f"Table {table_id} not found")
        cache_set(ck, data)
        return ok(data, "fbref")
    except HTTPException:
        raise
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
    lid = league_id.upper()
    if lid not in FBREF_LEAGUES:
        raise HTTPException(status_code=400, detail=f"Unsupported league: {league_id}")

    ck = cache_key("fbref", "team_season", league=lid, season=season, stat=stat_type)
    cached = cache_get(ck, TTL_STATS)
    if cached:
        return ok(cached, "fbref", cached=True)

    comp_id, comp_name = FBREF_LEAGUES[lid]
    season_str = format_season(season)
    stat_slug = stat_type if stat_type != "standard" else "stats"
    url = f"https://fbref.com/en/comps/{comp_id}/{season_str}/{stat_slug}/{season_str}-{comp_name}-Stats"

    try:
        soup = fbref_get(url)
        # Team tables have "for" prefix
        table_id = f"stats_squads_{stat_type}_for"
        if stat_type == "standard":
            table_id = "stats_squads_standard_for"
        data = parse_table(soup, table_id)
        if not data:
            raise HTTPException(status_code=502, detail=f"Team table not found for {stat_type}")
        cache_set(ck, data)
        return ok(data, "fbref")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
