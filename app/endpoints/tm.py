"""
Transfermarkt endpoints — requests + BeautifulSoup

Endpoints:
  GET /tm/player/{tm_id}/profile
  GET /tm/player/{tm_id}/transfers
  GET /tm/player/{tm_id}/market-value
  GET /tm/player/{tm_id}/stats
  GET /tm/club/{tm_id}/squad
  GET /tm/competition/{comp_id}/standings
  GET /tm/competition/{comp_id}/fixtures
"""

import re
import json
import time
import random
import requests
from bs4 import BeautifulSoup
from fastapi import APIRouter, HTTPException, Query
from app.utils.common import ok, cache_key, cache_get, cache_set

router = APIRouter()

TM_BASE       = "https://www.transfermarkt.com"
TTL_PROFILE   = 86400   # 24h
TTL_LIVE      = 3600    # 1h — fixtures/standings
TTL_MV        = 43200   # 12h — market value

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.transfermarkt.com/",
}

COMP_SLUG = {
    "GB1": ("premier-league",        "GB1"),
    "GB2": ("championship",          "GB2"),
    "L1":  ("bundesliga",            "L1"),
    "IT1": ("serie-a",               "IT1"),
    "FR1": ("ligue-1",               "FR1"),
    "NL1": ("eredivisie",            "NL1"),
    "ES1": ("laliga",                "ES1"),
    "CL":  ("uefa-champions-league", "CL"),
    "EL":  ("europa-league",         "EL"),
    "MLS": ("major-league-soccer",   "MLS"),
    "SA":  ("saudi-pro-league",      "SA"),
    "AL":  ("a-league-men",          "AL"),
    "JP1": ("j1-league",             "JP1"),
    "KR1": ("k-league-1",            "KR1"),
    "TH1": ("thai-league",           "TH1"),
    "VN1": ("v-league-1",            "VN1"),
    "MY1": ("super-league-malaysia", "MY1"),
}

def fetch(url: str) -> BeautifulSoup:
    time.sleep(random.uniform(2.0, 4.0))
    r = requests.get(url, headers=HEADERS, timeout=20)
    if r.status_code == 403:
        raise HTTPException(status_code=403, detail="Transfermarkt blocked request")
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def t(s) -> str:
    return s.strip() if s else ""

def clean_name(raw: str) -> str:
    """Remove shirt number prefix like '#4 ' from name."""
    return re.sub(r"^#\d+\s*", "", raw).strip()

def clean_market_value(raw: str) -> dict:
    """Parse '€65.00m Last update: 09/12/2025' into structured dict."""
    mv = {"raw": raw, "value": None, "currency": None, "unit": None, "last_update": None}
    m = re.search(r"([€$£])([0-9,.]+)([mk]?)", raw, re.IGNORECASE)
    if m:
        mv["currency"] = m.group(1)
        mv["value"]    = float(m.group(2).replace(",", ""))
        mv["unit"]     = m.group(3).lower() or "unit"
    upd = re.search(r"Last update:\s*(\S+)", raw)
    if upd:
        mv["last_update"] = upd.group(1)
    return mv


# ─────────────────────────────────────────
# PLAYER PROFILE
# ─────────────────────────────────────────

@router.get("/player/{tm_id}/profile")
def player_profile(tm_id: str):
    ck = cache_key("tm", "profile", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/x/profil/spieler/{tm_id}")

    name_tag = soup.find("h1", class_="data-header__headline-wrapper")
    name = clean_name(" ".join(name_tag.stripped_strings)) if name_tag else None

    info = {}
    for row in soup.select("span.info-table__content--bold"):
        label = row.find_previous_sibling("span")
        if label:
            info[t(label.text)] = t(row.text)

    mv_tag = soup.find("a", class_="data-header__market-value-wrapper")
    market_value = clean_market_value(t(mv_tag.text)) if mv_tag else None

    club_tag = soup.select_one("span.data-header__club a")
    club_href = club_tag["href"] if club_tag else None
    club_tm_id = club_href.split("/verein/")[1].split("/")[0] if club_href and "/verein/" in club_href else None

    nat_imgs = (
        soup.select("span.data-header__nationality img") or
        soup.select("div.data-header__nationality img") or
        soup.select("img.flaggenrahmen")
    )
    nationalities = [img.get("title", "") for img in nat_imgs if img.get("title")]

    data = {
        "tm_id":          tm_id,
        "name":           name,
        "nationalities":  nationalities,
        "market_value":   market_value,
        "club":           t(club_tag.text) if club_tag else None,
        "club_tm_id":     club_tm_id,
        "info":           info,
    }
    cache_set(ck, data)
    return ok(data, "tm")


# ─────────────────────────────────────────
# PLAYER TRANSFERS
# ─────────────────────────────────────────

@router.get("/player/{tm_id}/transfers")
def player_transfers(tm_id: str):
    ck = cache_key("tm", "transfers", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/x/transfers/spieler/{tm_id}")
    transfers = []
    for row in soup.select("div.transfer-record__entry"):
        from_el = row.select_one(".transfer-record__old-club a")
        to_el   = row.select_one(".transfer-record__new-club a")
        fee_el  = row.select_one(".transfer-record__fee")
        transfers.append({
            "season":    t(row.select_one(".transfer-record__season").text) if row.select_one(".transfer-record__season") else None,
            "date":      t(row.select_one(".transfer-record__date").text) if row.select_one(".transfer-record__date") else None,
            "from_club": t(from_el.text) if from_el else None,
            "to_club":   t(to_el.text) if to_el else None,
            "fee":       t(fee_el.text) if fee_el else None,
        })
    cache_set(ck, transfers)
    return ok(transfers, "tm")


# ─────────────────────────────────────────
# MARKET VALUE HISTORY
# ─────────────────────────────────────────

@router.get("/player/{tm_id}/market-value")
def market_value(tm_id: str):
    ck = cache_key("tm", "mv", id=tm_id)
    cached = cache_get(ck, TTL_MV)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/x/marktwertverlauf/spieler/{tm_id}")
    script = soup.find("script", string=re.compile(r"'data':"))
    if not script:
        raise HTTPException(status_code=404, detail="Market value data not found")
    raw = re.search(r"'data'\s*:\s*(\[.*?\])", script.string, re.DOTALL)
    points = json.loads(raw.group(1)) if raw else []
    data = {"tm_id": tm_id, "history": points}
    cache_set(ck, data)
    return ok(data, "tm")


# ─────────────────────────────────────────
# PLAYER STATS (season performance)
# ─────────────────────────────────────────

@router.get("/player/{tm_id}/stats")
def player_stats(tm_id: str, season_id: str = Query("2024", description="Season year, e.g. 2024")):
    ck = cache_key("tm", "stats", id=tm_id, season=season_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/x/leistungsdaten/spieler/{tm_id}/plus/0?saison={season_id}")
    rows = []
    for tr in soup.select("table.items tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 8:
            continue
        comp_a = tr.select_one("td.hauptlink a")
        rows.append({
            "competition":  t(comp_a.text) if comp_a else None,
            "appearances":  t(tds[3].text) if len(tds) > 3 else None,
            "goals":        t(tds[4].text) if len(tds) > 4 else None,
            "assists":      t(tds[5].text) if len(tds) > 5 else None,
            "minutes":      t(tds[7].text) if len(tds) > 7 else None,
        })
    rows = [r for r in rows if any(r.values())]
    cache_set(ck, rows)
    return ok(rows, "tm")


# ─────────────────────────────────────────
# CLUB SQUAD
# ─────────────────────────────────────────

@router.get("/club/{tm_id}/squad")
def club_squad(tm_id: str):
    ck = cache_key("tm", "squad", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/x/kader/verein/{tm_id}/saison_id/2024/plus/1")
    players = []
    for row in soup.select("table.items tbody tr.odd, table.items tbody tr.even"):
        name_a = row.select_one("td.hauptlink a")
        mv_td  = row.select_one("td.rechts.hauptlink")
        nat    = [img["title"] for img in row.select("td.zentriert img.flaggenrahmen")]
        href   = name_a["href"] if name_a else None
        pid    = href.split("/spieler/")[1].split("/")[0] if href and "/spieler/" in href else None
        pos_td = row.select_one("td.posrela table td")
        players.append({
            "tm_id":        pid,
            "name":         t(name_a.text) if name_a else None,
            "position":     t(pos_td.text) if pos_td else None,
            "nationality":  nat,
            "market_value": t(mv_td.text) if mv_td else None,
        })
    cache_set(ck, players)
    return ok(players, "tm")


# ─────────────────────────────────────────
# COMPETITION STANDINGS
# ─────────────────────────────────────────

@router.get("/competition/{comp_id}/standings")
def standings(comp_id: str):
    lid = comp_id.upper()
    if lid not in COMP_SLUG:
        raise HTTPException(status_code=400, detail=f"Unsupported competition: {comp_id}")
    slug, code = COMP_SLUG[lid]

    ck = cache_key("tm", "standings", comp=lid)
    cached = cache_get(ck, TTL_LIVE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/{slug}/tabelle/wettbewerb/{code}")
    table = []
    for row in soup.select("table.items tbody tr"):
        tds = row.find_all("td")
        if len(tds) < 10 or not tds[0].text.strip().isdigit():
            continue
        club_a = tds[2].find("a")
        href   = club_a["href"] if club_a else None
        cid    = href.split("/verein/")[1].split("/")[0] if href and "/verein/" in href else None
        table.append({
            "position":        int(tds[0].text.strip()),
            "club":            t(club_a.text) if club_a else None,
            "club_tm_id":      cid,
            "played":          t(tds[3].text),
            "won":             t(tds[4].text),
            "drawn":           t(tds[5].text),
            "lost":            t(tds[6].text),
            "goals_for":       t(tds[7].text).split(":")[0],
            "goals_against":   t(tds[7].text).split(":")[1] if ":" in tds[7].text else None,
            "goal_difference": t(tds[8].text),
            "points":          t(tds[9].text),
        })
    cache_set(ck, table)
    return ok(table, "tm")


# ─────────────────────────────────────────
# COMPETITION FIXTURES / RESULTS
# ─────────────────────────────────────────

@router.get("/competition/{comp_id}/fixtures")
def fixtures(
    comp_id:  str,
    matchday: str = Query("current", description="current | previous | next"),
):
    lid = comp_id.upper()
    if lid not in COMP_SLUG:
        raise HTTPException(status_code=400, detail=f"Unsupported competition: {comp_id}")
    slug, code = COMP_SLUG[lid]

    tab_map = {"previous": "spieltagtabs-1", "current": "spieltagtabs-2", "next": "spieltagtabs-3"}
    tab_id  = tab_map.get(matchday, "spieltagtabs-2")

    ck = cache_key("tm", "fixtures", comp=lid, md=matchday)
    cached = cache_get(ck, TTL_LIVE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/{slug}/startseite/wettbewerb/{code}")
    tab  = soup.find(id=tab_id)
    if not tab:
        return ok([], "tm")

    out = []
    for row in tab.select("table tr"):
        home_td   = row.find("td", class_="verein-heim")
        result_td = row.find("td", class_="ergebnis")
        away_td   = row.find("td", class_="verein-gast")
        if not (home_td and result_td and away_td):
            continue
        # Home: last <a> with /verein/ in href
        home_links = [a for a in home_td.find_all("a") if "/verein/" in a.get("href","")]
        home_a = home_links[-1] if home_links else home_td.find("a")
        # Away: first <a> with /verein/ in href
        away_links = [a for a in away_td.find_all("a") if "/verein/" in a.get("href","")]
        away_a = away_links[0] if away_links else away_td.find("a")
        home_id = home_a["href"].split("/verein/")[1].split("/")[0] if home_a and "/verein/" in home_a.get("href","") else None
        away_id = away_a["href"].split("/verein/")[1].split("/")[0] if away_a and "/verein/" in away_a.get("href","") else None
        result_a = result_td.find("a")
        result   = t(result_td.text)
        game_id  = None
        if result_a and "/spielbericht/" in result_a.get("href", ""):
            game_id = result_a["href"].split("/spielbericht/index/spielbericht/")[1].split("/")[0]
        is_score = bool(re.match(r"^\d+:\d+$", result))
        out.append({
            "game_id":         game_id,
            "home_team":       t(home_a.text) if home_a else None,
            "home_team_tm_id": home_id,
            "away_team":       t(away_a.text) if away_a else None,
            "away_team_tm_id": away_id,
            "score":           result if is_score else None,
            "kickoff":         result if not is_score else None,
        })

    cache_set(ck, out)
    return ok(out, "tm")
