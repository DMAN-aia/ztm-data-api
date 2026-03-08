"""
Transfermarkt endpoints — ZTM Data API v21
All 13 endpoints per ZTM Football Data Model spec.

Endpoints:
  GET /tm/player/{tm_id}
  GET /tm/player/{tm_id}/stats
  GET /tm/player/{tm_id}/transfers
  GET /tm/player/{tm_id}/market-value-history
  GET /tm/player/{tm_id}/injuries
  GET /tm/player/{tm_id}/suspensions
  GET /tm/player/{tm_id}/national-team
  GET /tm/club/{tm_id}/squad
  GET /tm/competition/{comp_id}/standings
  GET /tm/competition/{comp_id}/fixtures
  GET /tm/match/{game_id}
  GET /tm/competitions
  GET /tm/clubs
"""

import re
import json
import time
import random
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from fastapi import APIRouter, HTTPException, Query
from app.utils.common import ok, cache_key, cache_get, cache_set

router = APIRouter()

TM_BASE     = "https://www.transfermarkt.com"
TTL_PROFILE = 86400   # 24h
TTL_LIVE    = 3600    # 1h
TTL_MV      = 43200   # 12h

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

COMPETITIONS_METADATA = [
    {"competition_tm_id": "GB1", "competition_name": "Premier League",        "country": "England",      "tier": 1},
    {"competition_tm_id": "GB2", "competition_name": "Championship",          "country": "England",      "tier": 2},
    {"competition_tm_id": "L1",  "competition_name": "Bundesliga",            "country": "Germany",      "tier": 1},
    {"competition_tm_id": "IT1", "competition_name": "Serie A",               "country": "Italy",        "tier": 1},
    {"competition_tm_id": "FR1", "competition_name": "Ligue 1",               "country": "France",       "tier": 1},
    {"competition_tm_id": "NL1", "competition_name": "Eredivisie",            "country": "Netherlands",  "tier": 1},
    {"competition_tm_id": "ES1", "competition_name": "La Liga",               "country": "Spain",        "tier": 1},
    {"competition_tm_id": "CL",  "competition_name": "UEFA Champions League", "country": "Europe",       "tier": 0},
    {"competition_tm_id": "EL",  "competition_name": "UEFA Europa League",    "country": "Europe",       "tier": 0},
    {"competition_tm_id": "MLS", "competition_name": "Major League Soccer",   "country": "USA",          "tier": 1},
    {"competition_tm_id": "SA",  "competition_name": "Saudi Pro League",      "country": "Saudi Arabia", "tier": 1},
    {"competition_tm_id": "AL",  "competition_name": "A-League Men",          "country": "Australia",    "tier": 1},
    {"competition_tm_id": "JP1", "competition_name": "J1 League",             "country": "Japan",        "tier": 1},
    {"competition_tm_id": "KR1", "competition_name": "K League 1",            "country": "South Korea",  "tier": 1},
    {"competition_tm_id": "TH1", "competition_name": "Thai League",           "country": "Thailand",     "tier": 1},
    {"competition_tm_id": "VN1", "competition_name": "V.League 1",            "country": "Vietnam",      "tier": 1},
    {"competition_tm_id": "MY1", "competition_name": "Super League Malaysia", "country": "Malaysia",     "tier": 1},
]


# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

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
    return re.sub(r"^#\d+\s*", "", raw).strip()

def parse_market_value(raw: str) -> dict:
    out = {"value": None, "currency": None, "unit": None}
    if not raw:
        return out
    m = re.search(r"([€$£])([0-9,.]+)\s*([mk]?)", raw.replace(",", ""), re.IGNORECASE)
    if m:
        out["currency"] = m.group(1)
        try:
            out["value"] = float(m.group(2))
        except ValueError:
            pass
        out["unit"] = m.group(3).lower() if m.group(3) else "unit"
    return out

def extract_id(href, segment):
    if not href or segment not in href:
        return None
    try:
        return href.split(segment)[1].split("/")[0]
    except IndexError:
        return None

def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"

def parse_minute(raw: str):
    if not raw:
        return None
    m = re.search(r"\d+", raw)
    return int(m.group()) if m else None

def parse_status(score_raw: str) -> str:
    if not score_raw:
        return "scheduled"
    sl = score_raw.lower()
    if re.match(r"^\d+:\d+$", score_raw.strip()):
        return "finished"
    if "aet" in sl or "a.e.t" in sl:
        return "aet"
    if "pen" in sl:
        return "penalties"
    if "postponed" in sl or "pp" in sl:
        return "postponed"
    if "abandoned" in sl:
        return "abandoned"
    if "live" in sl or "'" in score_raw:
        return "live"
    return "scheduled"

def classify_transfer(fee_raw: str) -> str:
    if not fee_raw:
        return "transfer"
    fl = fee_raw.lower()
    if "loan" in fl and ("end" in fl or "return" in fl):
        return "loan_end"
    if "loan" in fl:
        return "loan"
    if "free" in fl or fl.strip() == "-":
        return "free_transfer"
    return "transfer"

def classify_suspension(raw: str) -> str:
    if not raw:
        return "disciplinary"
    rl = raw.lower()
    if "red card" in rl or "red-card" in rl:
        return "red_card"
    if "second yellow" in rl or "2nd yellow" in rl:
        return "second_yellow"
    if "accumulation" in rl or "5 yellow" in rl or "5th yellow" in rl:
        return "yellow_card_accumulation"
    return "disciplinary"

def parse_goal_type(raw: str) -> str:
    if not raw:
        return "goal"
    rl = raw.lower()
    if "penalty" in rl or "pen." in rl:
        return "penalty"
    if "own" in rl:
        return "own_goal"
    if "header" in rl:
        return "header"
    if "free kick" in rl or "free-kick" in rl:
        return "free_kick"
    return "goal"

def parse_assist_type(raw: str) -> str:
    if not raw:
        return "pass"
    rl = raw.lower()
    if "cross" in rl:
        return "cross"
    if "corner" in rl:
        return "corner"
    if "free kick" in rl:
        return "free_kick"
    if "penalty" in rl:
        return "penalty_won"
    return "pass"

def parse_card_type(raw: str) -> str:
    if not raw:
        return "yellow"
    rl = raw.lower()
    if "second" in rl or "2nd" in rl:
        return "second_yellow"
    if "red" in rl:
        return "red"
    return "yellow"


# ─────────────────────────────────────────
# 1. PLAYER PROFILE
# ─────────────────────────────────────────

@router.get("/player/{tm_id}")
def player_profile(tm_id: str):
    ck = cache_key("tm", "profile_v21", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/x/profil/spieler/{tm_id}")

    name_tag = soup.find("h1", class_="data-header__headline-wrapper")
    name = clean_name(" ".join(name_tag.stripped_strings)) if name_tag else None

    nat_imgs = (
        soup.select("span.data-header__nationality img") or
        soup.select("div.data-header__nationality img") or
        soup.select("img.flaggenrahmen")
    )
    nationalities = [img.get("title", "") for img in nat_imgs if img.get("title")]

    mv_tag = soup.find("a", class_="data-header__market-value-wrapper")
    mv_raw = t(mv_tag.text) if mv_tag else ""
    mv = parse_market_value(mv_raw)
    mv_last_update = None
    upd = re.search(r"Last update:\s*(\S+)", mv_raw)
    if upd:
        mv_last_update = upd.group(1)

    club_tag = soup.select_one("span.data-header__club a")
    club_href = club_tag["href"] if club_tag else None

    info = {}
    for row in soup.select("span.info-table__content--bold"):
        label = row.find_previous_sibling("span")
        if label:
            info[t(label.text)] = t(row.text)

    def get_info(*keys):
        for k in keys:
            for ik, iv in info.items():
                if k.lower() in ik.lower():
                    return iv
        return None

    data = {
        "tm_id":                    tm_id,
        "name":                     name,
        "date_of_birth":            get_info("Date of birth", "Born"),
        "age":                      get_info("Age"),
        "place_of_birth":           get_info("Place of birth", "Birthplace"),
        "nationalities":            nationalities,
        "height":                   get_info("Height"),
        "preferred_foot":           get_info("Foot", "Preferred foot"),
        "main_position":            get_info("Position", "Main position"),
        "secondary_positions":      get_info("Other position"),
        "current_club":             t(club_tag.text) if club_tag else None,
        "club_tm_id":               extract_id(club_href, "/verein/"),
        "contract_expires":         get_info("Contract expires", "Contract until"),
        "market_value":             mv["value"],
        "market_value_currency":    mv["currency"],
        "market_value_unit":        mv["unit"],
        "market_value_last_update": mv_last_update,
        "player_agent":             get_info("Player agent", "Agent"),
        "last_updated":             now_iso(),
    }
    cache_set(ck, data)
    return ok(data, "tm")


# ─────────────────────────────────────────
# 2. PLAYER SEASON STATISTICS
# ─────────────────────────────────────────

@router.get("/player/{tm_id}/stats")
def player_stats(tm_id: str, season_id: str = Query("2024", description="Season year e.g. 2024")):
    ck = cache_key("tm", "stats_v21", id=tm_id, season=season_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/x/leistungsdaten/spieler/{tm_id}/plus/0?saison={season_id}")
    rows = []
    for tr in soup.select("table.items tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 8:
            continue
        comp_a    = tr.select_one("td.hauptlink a")
        comp_href = comp_a["href"] if comp_a else None
        # Club column varies; try second hauptlink
        club_links = tr.select("td.hauptlink a")
        club_a     = club_links[1] if len(club_links) > 1 else None
        club_href  = club_a["href"] if club_a else None

        def td(i): return t(tds[i].text) if len(tds) > i else None

        rows.append({
            "season":            season_id,
            "competition":       t(comp_a.text) if comp_a else None,
            "competition_tm_id": extract_id(comp_href, "/wettbewerb/"),
            "club":              t(club_a.text) if club_a else None,
            "club_tm_id":        extract_id(club_href, "/verein/"),
            "appearances":       td(3),
            "goals":             td(4),
            "assists":           td(5),
            "minutes":           td(7),
            "yellow_cards":      td(8) if len(tds) > 8 else None,
            "red_cards":         td(9) if len(tds) > 9 else None,
        })
    rows = [r for r in rows if any(v for k, v in r.items() if k not in ("season",) and v)]
    cache_set(ck, rows)
    return ok(rows, "tm")


# ─────────────────────────────────────────
# 3. PLAYER TRANSFERS
# ─────────────────────────────────────────

@router.get("/player/{tm_id}/transfers")
def player_transfers(tm_id: str):
    ck = cache_key("tm", "transfers_v21", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/x/transfers/spieler/{tm_id}")
    transfers = []
    for row in soup.select("div.transfer-record__entry"):
        from_a  = row.select_one(".transfer-record__old-club a")
        to_a    = row.select_one(".transfer-record__new-club a")
        fee_el  = row.select_one(".transfer-record__fee")
        date_el = row.select_one(".transfer-record__date")
        seas_el = row.select_one(".transfer-record__season")
        fee_raw = t(fee_el.text) if fee_el else None
        mv      = parse_market_value(fee_raw or "")
        transfers.append({
            "season":          t(seas_el.text) if seas_el else None,
            "date":            t(date_el.text) if date_el else None,
            "from_club":       t(from_a.text) if from_a else None,
            "from_club_tm_id": extract_id(from_a["href"] if from_a else None, "/verein/"),
            "to_club":         t(to_a.text) if to_a else None,
            "to_club_tm_id":   extract_id(to_a["href"] if to_a else None, "/verein/"),
            "fee":             mv["value"],
            "currency":        mv["currency"],
            "transfer_type":   classify_transfer(fee_raw),
        })
    cache_set(ck, transfers)
    return ok(transfers, "tm")


# ─────────────────────────────────────────
# 4. PLAYER MARKET VALUE HISTORY
# ─────────────────────────────────────────

@router.get("/player/{tm_id}/market-value-history")
def market_value_history(tm_id: str):
    ck = cache_key("tm", "mv_v21", id=tm_id)
    cached = cache_get(ck, TTL_MV)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/x/marktwertverlauf/spieler/{tm_id}")
    script = soup.find("script", string=re.compile(r"'data':"))
    if not script:
        raise HTTPException(status_code=404, detail="Market value data not found")
    raw = re.search(r"'data'\s*:\s*(\[.*?\])", script.string, re.DOTALL)
    if not raw:
        raise HTTPException(status_code=404, detail="Market value data could not be parsed")
    points_raw = json.loads(raw.group(1))
    points = []
    for p in points_raw:
        points.append({
            "date":     p.get("datum_mw") or p.get("x"),
            "value":    p.get("y"),
            "currency": "€",
        })
    cache_set(ck, points)
    return ok(points, "tm")


# ─────────────────────────────────────────
# 5. PLAYER INJURIES
# ─────────────────────────────────────────

@router.get("/player/{tm_id}/injuries")
def player_injuries(tm_id: str):
    ck = cache_key("tm", "injuries_v21", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/x/verletzungen/spieler/{tm_id}")
    injuries = []
    for row in soup.select("table.items tbody tr"):
        tds = row.find_all("td")
        if len(tds) < 4:
            continue
        club_a = row.select_one("a[href*='/verein/']")
        injuries.append({
            "season":         t(tds[0].text) if len(tds) > 0 else None,
            "injury_type":    t(tds[1].text) if len(tds) > 1 else None,
            "start_date":     t(tds[2].text) if len(tds) > 2 else None,
            "end_date":       t(tds[3].text) if len(tds) > 3 else None,
            "matches_missed": t(tds[4].text) if len(tds) > 4 else None,
            "club":           t(club_a.text) if club_a else None,
            "club_tm_id":     extract_id(club_a["href"] if club_a else None, "/verein/"),
        })
    cache_set(ck, injuries)
    return ok(injuries, "tm")


# ─────────────────────────────────────────
# 6. PLAYER SUSPENSIONS
# ─────────────────────────────────────────

@router.get("/player/{tm_id}/suspensions")
def player_suspensions(tm_id: str):
    ck = cache_key("tm", "suspensions_v21", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/x/sperren/spieler/{tm_id}")
    suspensions = []
    for row in soup.select("table.items tbody tr"):
        tds = row.find_all("td")
        if len(tds) < 3:
            continue
        comp_a    = row.select_one("a[href*='/wettbewerb/']")
        reason_raw = t(tds[1].text) if len(tds) > 1 else None
        suspensions.append({
            "competition":       t(comp_a.text) if comp_a else None,
            "competition_tm_id": extract_id(comp_a["href"] if comp_a else None, "/wettbewerb/"),
            "start_date":        t(tds[2].text) if len(tds) > 2 else None,
            "end_date":          t(tds[3].text) if len(tds) > 3 else None,
            "matches_missed":    t(tds[4].text) if len(tds) > 4 else None,
            "reason":            classify_suspension(reason_raw),
            "reason_raw":        reason_raw,
        })
    cache_set(ck, suspensions)
    return ok(suspensions, "tm")


# ─────────────────────────────────────────
# 7. PLAYER NATIONAL TEAM STATS
# ─────────────────────────────────────────

@router.get("/player/{tm_id}/national-team")
def player_national_team(tm_id: str):
    ck = cache_key("tm", "national_v21", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/x/nationalmannschaft/spieler/{tm_id}")
    rows = []
    for tr in soup.select("table.items tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue
        comp_a    = tr.select_one("td.hauptlink a")
        comp_href = comp_a["href"] if comp_a else None

        def td(i): return t(tds[i].text) if len(tds) > i else None

        rows.append({
            "competition":       t(comp_a.text) if comp_a else None,
            "competition_tm_id": extract_id(comp_href, "/wettbewerb/"),
            "season":            td(1),
            "appearances":       td(2),
            "goals":             td(3),
            "assists":           td(4),
            "minutes":           td(5) if len(tds) > 5 else None,
        })
    rows = [r for r in rows if any(v for v in r.values() if v)]
    cache_set(ck, rows)
    return ok(rows, "tm")


# ─────────────────────────────────────────
# 8. CLUB SQUAD
# ─────────────────────────────────────────

@router.get("/club/{tm_id}/squad")
def club_squad(tm_id: str):
    ck = cache_key("tm", "squad_v21", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/x/kader/verein/{tm_id}/saison_id/2024/plus/1")
    players = []
    for row in soup.select("table.items tbody tr.odd, table.items tbody tr.even"):
        name_a    = row.select_one("td.hauptlink a")
        mv_td     = row.select_one("td.rechts.hauptlink")
        nat_imgs  = row.select("td.zentriert img.flaggenrahmen")
        href      = name_a["href"] if name_a else None
        pid       = extract_id(href, "/spieler/")
        pos_td    = row.select_one("td.posrela table td")
        shirt_td  = row.select_one("td.rn_nummer")
        mv_raw    = t(mv_td.text) if mv_td else ""
        mv        = parse_market_value(mv_raw)
        players.append({
            "player_tm_id":          pid,
            "name":                  clean_name(t(name_a.text)) if name_a else None,
            "shirt_number":          t(shirt_td.text) if shirt_td else None,
            "position":              t(pos_td.text) if pos_td else None,
            "nationality":           [img.get("title", "") for img in nat_imgs if img.get("title")],
            "market_value":          mv["value"],
            "market_value_currency": mv["currency"],
        })
    cache_set(ck, players)
    return ok(players, "tm")


# ─────────────────────────────────────────
# 9. COMPETITION STANDINGS
# ─────────────────────────────────────────

@router.get("/competition/{comp_id}/standings")
def standings(comp_id: str):
    lid = comp_id.upper()
    if lid not in COMP_SLUG:
        raise HTTPException(status_code=400, detail=f"Unsupported competition: {comp_id}")
    slug, code = COMP_SLUG[lid]

    ck = cache_key("tm", "standings_v21", comp=lid)
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
        gf_ga  = t(tds[7].text)
        table.append({
            "position":        int(tds[0].text.strip()),
            "club":            t(club_a.text) if club_a else None,
            "club_tm_id":      extract_id(club_a["href"] if club_a else None, "/verein/"),
            "played":          t(tds[3].text),
            "won":             t(tds[4].text),
            "drawn":           t(tds[5].text),
            "lost":            t(tds[6].text),
            "goals_for":       gf_ga.split(":")[0] if ":" in gf_ga else gf_ga,
            "goals_against":   gf_ga.split(":")[1] if ":" in gf_ga else None,
            "goal_difference": t(tds[8].text),
            "points":          t(tds[9].text),
        })
    cache_set(ck, table)
    return ok(table, "tm")


# ─────────────────────────────────────────
# 10. COMPETITION FIXTURES / RESULTS
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

    ck = cache_key("tm", "fixtures_v21", comp=lid, md=matchday)
    cached = cache_get(ck, TTL_LIVE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/{slug}/startseite/wettbewerb/{code}")
    tab  = soup.find(id=tab_id)
    if not tab:
        return ok([], "tm")

    comp_meta = next((c for c in COMPETITIONS_METADATA if c["competition_tm_id"] == lid), {})
    out = []
    for row in tab.select("table tr"):
        home_td   = row.find("td", class_="verein-heim")
        result_td = row.find("td", class_="ergebnis")
        away_td   = row.find("td", class_="verein-gast")
        if not (home_td and result_td and away_td):
            continue

        home_links = [a for a in home_td.find_all("a") if "/verein/" in a.get("href", "")]
        home_a     = home_links[-1] if home_links else home_td.find("a")
        away_links = [a for a in away_td.find_all("a") if "/verein/" in a.get("href", "")]
        away_a     = away_links[0] if away_links else away_td.find("a")

        result_a = result_td.find("a")
        result   = t(result_td.text)
        game_id  = None
        if result_a and "/spielbericht/" in result_a.get("href", ""):
            game_id = result_a["href"].split("/spielbericht/index/spielbericht/")[1].split("/")[0]

        is_score   = bool(re.match(r"^\d+:\d+$", result.strip()))
        home_score = result.split(":")[0] if is_score else None
        away_score = result.split(":")[1] if is_score else None

        out.append({
            "game_id":           game_id,
            "competition":       comp_meta.get("competition_name"),
            "competition_tm_id": lid,
            "season":            "2024",
            "round":             None,
            "home_team":         t(home_a.text) if home_a else None,
            "home_team_tm_id":   extract_id(home_a["href"] if home_a else None, "/verein/"),
            "away_team":         t(away_a.text) if away_a else None,
            "away_team_tm_id":   extract_id(away_a["href"] if away_a else None, "/verein/"),
            "home_score":        home_score,
            "away_score":        away_score,
            "kickoff_datetime":  result if not is_score else None,
            "status":            parse_status(result),
            "stadium":           None,
            "city":              None,
            "attendance":        None,
            "referee":           None,
        })

    cache_set(ck, out)
    return ok(out, "tm")


# ─────────────────────────────────────────
# 11. MATCH DETAILS
# ─────────────────────────────────────────

@router.get("/match/{game_id}")
def match_details(game_id: str):
    ck = cache_key("tm", "match_v21", id=game_id)
    cached = cache_get(ck, TTL_LIVE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/x/index/spielbericht/{game_id}")

    # ── Meta ──────────────────────────────
    home_a = soup.select_one("div.sb-team.sb-heim a[href*='/verein/']")
    away_a = soup.select_one("div.sb-team.sb-gast a[href*='/verein/']")
    score_el  = soup.select_one("div.sb-ergebnis")
    score_raw = re.sub(r"\s+", "", t(score_el.text)) if score_el else None
    home_score = score_raw.split(":")[0] if score_raw and ":" in score_raw else None
    away_score = score_raw.split(":")[1] if score_raw and ":" in score_raw else None

    comp_a    = soup.select_one("a.sb-wettbewerb")
    info_tds  = soup.select("div.spielbericht-head td")

    def find_info(*keywords):
        for kw in keywords:
            for td in info_tds:
                if kw.lower() in td.text.lower():
                    sib = td.find_next_sibling("td")
                    if sib:
                        return t(sib.text)
        return None

    home_tm_id = extract_id(home_a["href"] if home_a else None, "/verein/")
    away_tm_id = extract_id(away_a["href"] if away_a else None, "/verein/")

    meta = {
        "game_id":           game_id,
        "competition":       t(comp_a.text) if comp_a else None,
        "competition_tm_id": extract_id(comp_a["href"] if comp_a else None, "/wettbewerb/"),
        "season":            find_info("season", "saison"),
        "round":             find_info("matchday", "spieltag"),
        "kickoff_datetime":  find_info("kickoff", "date", "datum"),
        "stadium":           find_info("stadium", "stadion"),
        "city":              find_info("city", "stadt"),
        "attendance":        find_info("attendance", "zuschauer"),
        "referee":           find_info("referee", "schiedsrichter"),
        "home_team":         t(home_a.text) if home_a else None,
        "home_team_tm_id":   home_tm_id,
        "away_team":         t(away_a.text) if away_a else None,
        "away_team_tm_id":   away_tm_id,
        "home_score":        home_score,
        "away_score":        away_score,
        "status":            parse_status(score_raw or ""),
    }

    # ── Goals ─────────────────────────────
    goals = []
    for section_class, club_side in [("sb-aktion-heim", "home"), ("sb-aktion-gast", "away")]:
        for row in soup.select(f"div.{section_class} div.sb-aktion-aktion"):
            minute_el = row.select_one("div.sb-sprite-uhr-klein")
            player_as = row.select("a[href*='/spieler/']")
            scorer_a  = player_as[0] if player_as else None
            assist_a  = player_as[1] if len(player_as) > 1 else None
            type_el   = row.select_one("span.sb-aktion-icon")
            type_text = t(type_el.get("title", "") if type_el else "") or t(type_el.text if type_el else "")
            goals.append({
                "minute":       t(minute_el.text) if minute_el else None,
                "scorer_name":  t(scorer_a.text) if scorer_a else None,
                "scorer_tm_id": extract_id(scorer_a["href"] if scorer_a else None, "/spieler/"),
                "assist_name":  t(assist_a.text) if assist_a else None,
                "assist_tm_id": extract_id(assist_a["href"] if assist_a else None, "/spieler/"),
                "assist_type":  parse_assist_type(type_text),
                "goal_type":    parse_goal_type(type_text),
                "club":         meta["home_team"] if club_side == "home" else meta["away_team"],
                "club_tm_id":   home_tm_id if club_side == "home" else away_tm_id,
            })

    # ── Cards ─────────────────────────────
    cards = []
    for section_class, club_side in [("sb-aktion-heim", "home"), ("sb-aktion-gast", "away")]:
        for row in soup.select(f"div.{section_class} div.sb-aktion-karte"):
            minute_el = row.select_one("div.sb-sprite-uhr-klein")
            player_a  = row.select_one("a[href*='/spieler/']")
            card_el   = row.select_one("span.sb-aktion-icon")
            card_text = t(card_el.get("title", "") if card_el else "") or t(card_el.text if card_el else "")
            cards.append({
                "minute":       t(minute_el.text) if minute_el else None,
                "player_name":  t(player_a.text) if player_a else None,
                "player_tm_id": extract_id(player_a["href"] if player_a else None, "/spieler/"),
                "card_type":    parse_card_type(card_text),
                "club":         meta["home_team"] if club_side == "home" else meta["away_team"],
                "club_tm_id":   home_tm_id if club_side == "home" else away_tm_id,
            })

    # ── Lineups ───────────────────────────
    lineups = []
    for side, ctm_id in [("heim", home_tm_id), ("gast", away_tm_id)]:
        section = soup.select_one(f"div.aufstellung-{side}")
        if not section:
            continue
        # Starters vs bench distinguished by parent div class
        for row in section.select("table.items tr"):
            player_a   = row.select_one("a[href*='/spieler/']")
            if not player_a:
                continue
            pos_td     = row.select_one("td.posrela")
            shirt_td   = row.select_one("td.rn_nummer")
            is_starter = "aufstellung-startelelf" in " ".join(
                row.find_parent("div", class_=re.compile(r"aufstellung")).get("class", [])
                if row.find_parent("div", class_=re.compile(r"aufstellung")) else []
            )
            captain_el = row.find("span", class_=re.compile(r"kapitaen|captain"))
            lineups.append({
                "player_name":  clean_name(t(player_a.text)),
                "player_tm_id": extract_id(player_a["href"], "/spieler/"),
                "club_tm_id":   ctm_id,
                "position":     t(pos_td.text) if pos_td else None,
                "shirt_number": t(shirt_td.text) if shirt_td else None,
                "is_starting":  is_starter,
                "is_captain":   captain_el is not None,
            })

    # ── Substitutions ─────────────────────
    substitutions = []
    for section_class, club_side in [("sb-aktion-heim", "home"), ("sb-aktion-gast", "away")]:
        for row in soup.select(f"div.{section_class} div.sb-aktion-wechsel"):
            minute_el = row.select_one("div.sb-sprite-uhr-klein")
            player_as = row.select("a[href*='/spieler/']")
            out_a = player_as[0] if len(player_as) > 0 else None
            in_a  = player_as[1] if len(player_as) > 1 else None
            substitutions.append({
                "minute":           t(minute_el.text) if minute_el else None,
                "player_out_name":  t(out_a.text) if out_a else None,
                "player_out_tm_id": extract_id(out_a["href"] if out_a else None, "/spieler/"),
                "player_in_name":   t(in_a.text) if in_a else None,
                "player_in_tm_id":  extract_id(in_a["href"] if in_a else None, "/spieler/"),
                "club":             meta["home_team"] if club_side == "home" else meta["away_team"],
                "club_tm_id":       home_tm_id if club_side == "home" else away_tm_id,
            })

    # ── Player match stats (derived) ──────
    pm: dict[str, dict] = {}

    for p in lineups:
        pid = p["player_tm_id"]
        if not pid:
            continue
        pm[pid] = {
            "player_tm_id": pid,
            "club_tm_id":   p["club_tm_id"],
            "start_min":    0 if p["is_starting"] else None,
            "end_min":      90 if p["is_starting"] else None,
            "goals":        0,
            "assists":      0,
            "yellow_cards": 0,
            "red_cards":    0,
        }

    for sub in substitutions:
        mn = parse_minute(sub["minute"])
        if sub["player_out_tm_id"] and sub["player_out_tm_id"] in pm:
            pm[sub["player_out_tm_id"]]["end_min"] = mn
        if sub["player_in_tm_id"]:
            if sub["player_in_tm_id"] not in pm:
                pm[sub["player_in_tm_id"]] = {
                    "player_tm_id": sub["player_in_tm_id"],
                    "club_tm_id":   sub["club_tm_id"],
                    "start_min":    mn,
                    "end_min":      90,
                    "goals":        0,
                    "assists":      0,
                    "yellow_cards": 0,
                    "red_cards":    0,
                }
            else:
                pm[sub["player_in_tm_id"]]["start_min"] = mn
                pm[sub["player_in_tm_id"]]["end_min"]   = 90

    for g in goals:
        if g["scorer_tm_id"] and g["scorer_tm_id"] in pm:
            pm[g["scorer_tm_id"]]["goals"] += 1
        if g["assist_tm_id"] and g["assist_tm_id"] in pm:
            pm[g["assist_tm_id"]]["assists"] += 1

    for c in cards:
        pid = c["player_tm_id"]
        if pid and pid in pm:
            if c["card_type"] in ("red", "second_yellow"):
                pm[pid]["red_cards"] += 1
                cm = parse_minute(c["minute"])
                pm[pid]["end_min"] = cm
            else:
                pm[pid]["yellow_cards"] += 1

    player_match_stats = []
    for pid, d in pm.items():
        s, e = d["start_min"], d["end_min"]
        played = (e - s) if (s is not None and e is not None) else None
        player_match_stats.append({
            "player_tm_id":   pid,
            "club_tm_id":     d["club_tm_id"],
            "minutes_played": played,
            "goals":          d["goals"],
            "assists":        d["assists"],
            "yellow_cards":   d["yellow_cards"],
            "red_cards":      d["red_cards"],
        })

    data = {
        "meta":               meta,
        "goals":              goals,
        "cards":              cards,
        "lineups":            lineups,
        "substitutions":      substitutions,
        "player_match_stats": player_match_stats,
    }
    cache_set(ck, data)
    return ok(data, "tm")


# ─────────────────────────────────────────
# 12. COMPETITIONS METADATA
# ─────────────────────────────────────────

@router.get("/competitions")
def competitions():
    return ok(COMPETITIONS_METADATA, "tm")


# ─────────────────────────────────────────
# 13. CLUBS METADATA
# ─────────────────────────────────────────

@router.get("/clubs")
def clubs(comp_id: str = Query(..., description="Competition TM ID e.g. GB1")):
    lid = comp_id.upper()
    if lid not in COMP_SLUG:
        raise HTTPException(status_code=400, detail=f"Unsupported competition: {comp_id}")
    slug, code = COMP_SLUG[lid]

    ck = cache_key("tm", "clubs_v21", comp=lid)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/{slug}/startseite/wettbewerb/{code}")
    clubs_out = []
    seen = set()
    for row in soup.select("table.items tbody tr"):
        club_a = row.select_one("td.hauptlink a[href*='/verein/']")
        if not club_a:
            continue
        href = club_a.get("href", "")
        cid  = extract_id(href, "/verein/")
        if cid in seen:
            continue
        seen.add(cid)
        stad_td  = row.select_one("td:nth-child(5)")
        found_td = row.select_one("td:nth-child(6)")
        clubs_out.append({
            "club_tm_id": cid,
            "club_name":  t(club_a.text),
            "country":    None,
            "stadium":    t(stad_td.text) if stad_td else None,
            "founded":    t(found_td.text) if found_td else None,
        })
    cache_set(ck, clubs_out)
    return ok(clubs_out, "tm")
