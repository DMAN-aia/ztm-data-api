"""
Transfermarkt endpoints — direct BeautifulSoup scraping
Kept separate from soccerdata because TM data (market values, transfers, profiles)
is not available in any soccerdata source.

Available endpoints:
  GET /tm/player/{tm_id}/profile
  GET /tm/player/{tm_id}/transfers
  GET /tm/player/{tm_id}/market-value
  GET /tm/club/{tm_id}/squad
  GET /tm/competitions/{comp_id}/standings
  GET /tm/competitions/{comp_id}/matches
"""

import re
import time
import random
import requests
from bs4 import BeautifulSoup
from fastapi import APIRouter, HTTPException, Query
from app.utils.common import ok, cache_key, cache_get, cache_set

router = APIRouter()

TM_BASE    = "https://www.transfermarkt.com"
TTL_PROFILE  = 86400    # 24 hours
TTL_MATCHES  = 3600     # 1 hour
TTL_STANDINGS = 3600

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

COMP_SLUG = {
    "GB1": "premier-league",
    "GB2": "championship",
    "L1":  "bundesliga",
    "IT1": "serie-a",
    "FR1": "ligue-1",
    "NL1": "eredivisie",
    "ES1": "laliga",
    "CL":  "uefa-champions-league",
    "EL":  "europa-league",
    "MLS": "major-league-soccer",
    "SA":  "saudi-pro-league",
    "AL":  "a-league-men",
    "JP1": "j1-league",
    "KR1": "k-league-1",
    "TH1": "thai-league",
    "VN1": "v-league-1",
    "MY1": "super-league-malaysia",
}


def fetch(url: str) -> BeautifulSoup:
    time.sleep(random.uniform(2.0, 4.0))
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")


def trim(s) -> str:
    return s.strip() if s else ""


# ─────────────────────────────────────────
# PLAYER PROFILE
# ─────────────────────────────────────────

@router.get("/player/{tm_id}/profile")
def player_profile(tm_id: str):
    ck = cache_key("tm", "player_profile", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    try:
        soup = fetch(f"{TM_BASE}/x/profil/spieler/{tm_id}")
        name_tag = soup.find("h1", class_="data-header__headline-wrapper")
        name = " ".join(name_tag.stripped_strings) if name_tag else None

        info = {}
        for row in soup.select("span.info-table__content--bold"):
            label_el = row.find_previous_sibling("span")
            if label_el:
                info[trim(label_el.text)] = trim(row.text)

        mv_tag = soup.find("a", class_="data-header__market-value-wrapper")
        market_value = trim(mv_tag.text) if mv_tag else None

        club_tag = soup.select_one("span.data-header__club a")
        club = trim(club_tag.text) if club_tag else None
        club_href = club_tag["href"] if club_tag else None
        club_tm_id = club_href.split("/verein/")[1].split("/")[0] if club_href and "/verein/" in club_href else None

        data = {
            "tm_id": tm_id,
            "name": name,
            "market_value": market_value,
            "club": club,
            "club_tm_id": club_tm_id,
            "info": info,
        }
        cache_set(ck, data)
        return ok(data, "tm")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────
# PLAYER TRANSFERS
# ─────────────────────────────────────────

@router.get("/player/{tm_id}/transfers")
def player_transfers(tm_id: str):
    ck = cache_key("tm", "player_transfers", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    try:
        soup = fetch(f"{TM_BASE}/x/transfers/spieler/{tm_id}")
        transfers = []
        for row in soup.select("div.transfer-record__entry"):
            season  = trim(row.select_one(".transfer-record__season").text) if row.select_one(".transfer-record__season") else None
            date    = trim(row.select_one(".transfer-record__date").text) if row.select_one(".transfer-record__date") else None
            from_el = row.select_one(".transfer-record__old-club a")
            to_el   = row.select_one(".transfer-record__new-club a")
            fee_el  = row.select_one(".transfer-record__fee")
            transfers.append({
                "season":       season,
                "date":         date,
                "from_club":    trim(from_el.text) if from_el else None,
                "to_club":      trim(to_el.text) if to_el else None,
                "fee":          trim(fee_el.text) if fee_el else None,
            })
        cache_set(ck, transfers)
        return ok(transfers, "tm")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────
# MARKET VALUE HISTORY
# ─────────────────────────────────────────

@router.get("/player/{tm_id}/market-value")
def market_value(tm_id: str):
    ck = cache_key("tm", "market_value", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    try:
        soup = fetch(f"{TM_BASE}/x/marktwertverlauf/spieler/{tm_id}")
        # MV history is in a JS array — extract it
        script = soup.find("script", string=re.compile("'data':"))
        if not script:
            raise HTTPException(status_code=404, detail="Market value data not found")
        raw = re.search(r"'data'\s*:\s*(\[.*?\])", script.string, re.DOTALL)
        import json
        points = json.loads(raw.group(1)) if raw else []
        data = {"tm_id": tm_id, "history": points}
        cache_set(ck, data)
        return ok(data, "tm")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────
# CLUB SQUAD
# ─────────────────────────────────────────

@router.get("/club/{tm_id}/squad")
def club_squad(tm_id: str):
    ck = cache_key("tm", "club_squad", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    try:
        soup = fetch(f"{TM_BASE}/x/kader/verein/{tm_id}/saison_id/2024/plus/1")
        players = []
        for row in soup.select("table.items tbody tr.odd, table.items tbody tr.even"):
            tds = row.find_all("td")
            if len(tds) < 5:
                continue
            name_a = row.select_one("td.hauptlink a")
            mv_td  = row.select_one("td.rechts.hauptlink")
            nat    = [img["title"] for img in row.select("td.zentriert img.flaggenrahmen")] if row else []
            href   = name_a["href"] if name_a else None
            pid    = href.split("/spieler/")[1].split("/")[0] if href and "/spieler/" in href else None
            players.append({
                "tm_id":        pid,
                "name":         trim(name_a.text) if name_a else None,
                "nationality":  nat,
                "market_value": trim(mv_td.text) if mv_td else None,
            })
        cache_set(ck, players)
        return ok(players, "tm")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────
# COMPETITION STANDINGS
# ─────────────────────────────────────────

@router.get("/competitions/{comp_id}/standings")
def standings(comp_id: str):
    slug = COMP_SLUG.get(comp_id.upper())
    if not slug:
        raise HTTPException(status_code=400, detail=f"Unsupported competition: {comp_id}")

    ck = cache_key("tm", "standings", comp=comp_id)
    cached = cache_get(ck, TTL_STANDINGS)
    if cached:
        return ok(cached, "tm", cached=True)

    try:
        soup = fetch(f"{TM_BASE}/{slug}/tabelle/wettbewerb/{comp_id}")
        table = []
        for row in soup.select("table.items tbody tr"):
            tds = row.find_all("td")
            if len(tds) < 10 or not tds[0].text.strip().isdigit():
                continue
            club_a = tds[2].find("a")
            href   = club_a["href"] if club_a else None
            cid    = href.split("/verein/")[1].split("/")[0] if href and "/verein/" in href else None
            table.append({
                "position":       int(tds[0].text.strip()),
                "club_name":      trim(club_a.text) if club_a else None,
                "club_tm_id":     cid,
                "played":         trim(tds[3].text),
                "won":            trim(tds[4].text),
                "drawn":          trim(tds[5].text),
                "lost":           trim(tds[6].text),
                "goals_for":      trim(tds[7].text).split(":")[0],
                "goals_against":  trim(tds[7].text).split(":")[1] if ":" in tds[7].text else None,
                "goal_difference":trim(tds[8].text),
                "points":         trim(tds[9].text),
            })
        cache_set(ck, table)
        return ok(table, "tm")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────
# COMPETITION MATCHES (current matchday)
# ─────────────────────────────────────────

@router.get("/competitions/{comp_id}/matches")
def matches(
    comp_id: str,
    matchday: str = Query("current", description="current | previous | next"),
):
    slug = COMP_SLUG.get(comp_id.upper())
    if not slug:
        raise HTTPException(status_code=400, detail=f"Unsupported competition: {comp_id}")

    tab_map = {"previous": "spieltagtabs-1", "current": "spieltagtabs-2", "next": "spieltagtabs-3"}
    tab_id  = tab_map.get(matchday, "spieltagtabs-2")

    ck = cache_key("tm", "matches", comp=comp_id, md=matchday)
    cached = cache_get(ck, TTL_MATCHES)
    if cached:
        return ok(cached, "tm", cached=True)

    try:
        soup  = fetch(f"{TM_BASE}/{slug}/startseite/wettbewerb/{comp_id}")
        tab   = soup.find(id=tab_id)
        if not tab:
            return ok([], "tm")

        matches_out = []
        for row in tab.select("table tr"):
            home_td  = row.find("td", class_="verein-heim")
            result_td = row.find("td", class_="ergebnis")
            away_td  = row.find("td", class_="verein-gast")
            if not (home_td and result_td and away_td):
                continue

            home_text = trim(home_td.text)
            away_text = trim(away_td.text)
            home_name = home_text.split(")")[-1].strip() if ")" in home_text else home_text
            away_name = away_text.split("(")[0].strip() if "(" in away_text else away_text

            home_a   = home_td.find("a")
            away_a   = away_td.find("a")
            home_id  = home_a["href"].split("/verein/")[1].split("/")[0] if home_a and "/verein/" in home_a.get("href","") else None
            away_id  = away_a["href"].split("/verein/")[1].split("/")[0] if away_a and "/verein/" in away_a.get("href","") else None

            result_a = result_td.find("a")
            result   = trim(result_td.text)
            game_id  = None
            if result_a and "/spielbericht/" in result_a.get("href", ""):
                game_id = result_a["href"].split("/spielbericht/index/spielbericht/")[1].split("/")[0]

            is_score = bool(re.match(r"^\d+:\d+$", result))

            matches_out.append({
                "game_id":         game_id,
                "home_team":       home_name,
                "home_team_tm_id": home_id,
                "away_team":       away_name,
                "away_team_tm_id": away_id,
                "score":           result if is_score else None,
                "kickoff":         result if not is_score else None,
            })

        cache_set(ck, matches_out)
        return ok(matches_out, "tm")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
