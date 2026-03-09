"""
Transfermarkt endpoints — ZTM Data API v22
Basis: v20 (getest, werkend). Delta: nieuwe velden op bestaande endpoints + 6 nieuwe endpoints.

Endpoints (bestaand, uitgebreid):
  GET /tm/player/{tm_id}                   (was /profile — extra velden)
  GET /tm/player/{tm_id}/stats             (+ yellow_cards, red_cards, competition_tm_id, club_tm_id)
  GET /tm/player/{tm_id}/transfers         (JSON API + from/to tm_id, fee split, transfer_type)
  GET /tm/player/{tm_id}/market-value-history  (JSON API, was /market-value)
  GET /tm/club/{tm_id}/squad               (+ shirt_number, market_value_currency)
  GET /tm/competition/{comp_id}/standings  (ongewijzigd)
  GET /tm/competition/{comp_id}/fixtures   (+ home_score/away_score split, status, competition velden)

Endpoints (nieuw):
  GET /tm/player/{tm_id}/injuries
  GET /tm/player/{tm_id}/suspensions
  GET /tm/player/{tm_id}/national-team
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
TM_CEAPI    = "https://www.transfermarkt.com/ceapi"
TTL_PROFILE = 86400   # 24h
TTL_LIVE    = 3600    # 1h
TTL_MV      = 43200   # 12h

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.transfermarkt.com/",
}

# competition_tm_id → (url-slug, api-code)
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
# HELPERS (v20 basis, ongewijzigd)
# ─────────────────────────────────────────

def fetch(url: str) -> BeautifulSoup:
    time.sleep(random.uniform(2.0, 4.0))
    r = requests.get(url, headers=HEADERS, timeout=20)
    if r.status_code == 403:
        raise HTTPException(status_code=403, detail="Transfermarkt blocked request")
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def fetch_json(url: str) -> dict:
    """Voor TM ceapi JSON endpoints — geen sleep nodig, snelle JSON response."""
    time.sleep(random.uniform(1.0, 2.0))
    r = requests.get(url, headers=HEADERS, timeout=20)
    if r.status_code == 403:
        raise HTTPException(status_code=403, detail="Transfermarkt blocked request")
    r.raise_for_status()
    return r.json()

def t(s) -> str:
    return s.strip() if s else ""

def clean_name(raw: str) -> str:
    """Verwijder rugnummer prefix zoals '#4 ' — v20 bewezen."""
    return re.sub(r"^#\d+\s*", "", raw).strip()

def clean_market_value(raw: str) -> dict:
    """Parse '€65.00m Last update: 09/12/2025' — v20 bewezen."""
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

def extract_id(href: str, segment: str):
    """Extract TM numeric ID uit href. e.g. extract_id(href, '/verein/')"""
    if not href or segment not in href:
        return None
    try:
        return href.split(segment)[1].split("/")[0]
    except IndexError:
        return None

def parse_minute(raw: str):
    if not raw:
        return None
    m = re.search(r"\d+", raw)
    return int(m.group()) if m else None

def parse_status(score_raw: str) -> str:
    if not score_raw:
        return "scheduled"
    sl = score_raw.lower().strip()
    if re.match(r"^\d+:\d+$", sl):
        return "finished"
    if "aet" in sl or "a.e.t" in sl:
        return "aet"
    if "pen" in sl:
        return "penalties"
    if "postponed" in sl or "pp" in sl:
        return "postponed"
    if "abandoned" in sl:
        return "abandoned"
    if "'" in score_raw:
        return "live"
    return "scheduled"

def classify_transfer_type(fee_raw: str) -> str:
    if not fee_raw:
        return "transfer"
    fl = fee_raw.lower()
    if "loan" in fl and ("end" in fl or "return" in fl):
        return "loan_end"
    if "loan" in fl:
        return "loan"
    if "free" in fl or fl.strip() in ("-", "free transfer"):
        return "free_transfer"
    return "transfer"

def classify_suspension_reason(raw: str) -> str:
    if not raw:
        return "disciplinary"
    rl = raw.lower()
    if "red card" in rl:
        return "red_card"
    if "second yellow" in rl or "2nd yellow" in rl:
        return "second_yellow"
    if "accumulation" in rl or "5 yellow" in rl or "5th yellow" in rl:
        return "yellow_card_accumulation"
    return "disciplinary"

def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


# ─────────────────────────────────────────
# 1. PLAYER PROFILE
# v20 basis + extra velden uit info-tabel
# ─────────────────────────────────────────

@router.get("/player/{tm_id}")
def player_profile(tm_id: str):
    ck = cache_key("tm", "profile_v22", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/x/profil/spieler/{tm_id}")

    # Naam — v20 bewezen
    name_tag = soup.find("h1", class_="data-header__headline-wrapper")
    name = clean_name(" ".join(name_tag.stripped_strings)) if name_tag else None

    # Info tabel — v20 bewezen
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

    # Market value — v20 bewezen
    mv_tag = soup.find("a", class_="data-header__market-value-wrapper")
    mv_raw = t(mv_tag.text) if mv_tag else ""
    mv = clean_market_value(mv_raw)

    # Club — v20 bewezen
    club_tag = soup.select_one("span.data-header__club a")
    club_href = club_tag["href"] if club_tag else None

    # Nationalities — v20 fix (drie fallbacks) bewezen
    nat_imgs = (
        soup.select("span.data-header__nationality img") or
        soup.select("div.data-header__nationality img") or
        soup.select("img.flaggenrahmen")
    )
    nationalities = [img.get("title", "") for img in nat_imgs if img.get("title")]

    # XPath selectors uit felipeall voor specifieke profielvelden
    dob_el = soup.select_one("span[itemprop='birthDate']")
    place_els = soup.select("span:-soup-contains('Place of birth') ~ span")

    data = {
        "tm_id":                    tm_id,
        "name":                     name,
        "date_of_birth":            get_info("Date of birth", "Born"),
        "age":                      get_info("Age"),
        "place_of_birth":           get_info("Place of birth", "Birthplace"),
        "nationalities":            nationalities,
        "height":                   get_info("Height"),
        "preferred_foot":           get_info("Foot"),
        "main_position":            get_info("Main position", "Position"),
        "secondary_positions":      get_info("Other position"),
        "current_club":             t(club_tag.text) if club_tag else None,
        "club_tm_id":               extract_id(club_href, "/verein/"),
        "contract_expires":         get_info("Contract expires", "Contract until"),
        "market_value":             mv["value"],
        "market_value_currency":    mv["currency"],
        "market_value_unit":        mv["unit"],
        "market_value_last_update": mv["last_update"],
        "player_agent":             get_info("Player agent"),
        "last_updated":             now_iso(),
    }
    cache_set(ck, data)
    return ok(data, "tm")


# ─────────────────────────────────────────
# 2. PLAYER STATS
# v20 basis + yellow_cards, red_cards, competition_tm_id, club_tm_id
# ─────────────────────────────────────────

@router.get("/player/{tm_id}/stats")
def player_stats(tm_id: str, season_id: str = Query("2024", description="Season jaar, bijv. 2024")):
    ck = cache_key("tm", "stats_v22", id=tm_id, season=season_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/x/leistungsdaten/spieler/{tm_id}/plus/0?saison={season_id}")
    rows = []
    for tr in soup.select("table.items tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 8:
            continue
        # Competition — v20 bewezen selector
        comp_a    = tr.select_one("td.hauptlink a")
        comp_href = comp_a["href"] if comp_a else None
        # Club — tweede hauptlink link
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
    rows = [r for r in rows if any(v for k, v in r.items() if k != "season" and v)]
    cache_set(ck, rows)
    return ok(rows, "tm")


# ─────────────────────────────────────────
# 3. PLAYER TRANSFERS
# Nieuw: ceapi JSON endpoint (betrouwbaarder dan HTML)
# ─────────────────────────────────────────

@router.get("/player/{tm_id}/transfers")
def player_transfers(tm_id: str):
    ck = cache_key("tm", "transfers_v22", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    data = fetch_json(f"{TM_CEAPI}/transferHistory/list/{tm_id}")
    transfers_raw = data.get("transfers", [])

    transfers = []
    for tr in transfers_raw:
        fee_raw  = tr.get("fee", "")
        mv_raw   = tr.get("marketValue", "")
        fee_mv   = clean_market_value(fee_raw or "")
        from_club = tr.get("from", {})
        to_club   = tr.get("to", {})
        transfers.append({
            "season":          tr.get("season"),
            "date":            tr.get("date"),
            "from_club":       from_club.get("clubName"),
            "from_club_tm_id": extract_id(from_club.get("href", ""), "/verein/"),
            "to_club":         to_club.get("clubName"),
            "to_club_tm_id":   extract_id(to_club.get("href", ""), "/verein/"),
            "fee":             fee_mv["value"],
            "currency":        fee_mv["currency"],
            "transfer_type":   classify_transfer_type(fee_raw),
        })
    cache_set(ck, transfers)
    return ok(transfers, "tm")


# ─────────────────────────────────────────
# 4. PLAYER MARKET VALUE HISTORY
# Nieuw: ceapi JSON endpoint (was /market-value via Highcharts regex)
# ─────────────────────────────────────────

@router.get("/player/{tm_id}/market-value-history")
def market_value_history(tm_id: str):
    ck = cache_key("tm", "mv_v22", id=tm_id)
    cached = cache_get(ck, TTL_MV)
    if cached:
        return ok(cached, "tm", cached=True)

    data = fetch_json(f"{TM_CEAPI}/marketValueDevelopment/graph/{tm_id}")
    points_raw = data.get("list", [])
    points = []
    for p in points_raw:
        points.append({
            "date":     p.get("datum_mw"),
            "value":    p.get("mw"),
            "currency": "€",  # TM is altijd EUR
        })
    cache_set(ck, points)
    return ok(points, "tm")


# ─────────────────────────────────────────
# 5. PLAYER INJURIES (nieuw)
# Selectors: felipeall xpath → div#yw1 tbody tr, td[1..6]
# ─────────────────────────────────────────

@router.get("/player/{tm_id}/injuries")
def player_injuries(tm_id: str):
    ck = cache_key("tm", "injuries_v22", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/x/verletzungen/spieler/{tm_id}/plus/1")
    injuries = []
    for row in soup.select("div#yw1 table.items tbody tr"):
        tds = row.find_all("td")
        if len(tds) < 5:
            continue
        # gamesMissedClubs: links in td[6] naar /verein/
        club_links = tds[5].select("a[href*='/verein/']") if len(tds) > 5 else []
        club_ids = [extract_id(a["href"], "/verein/") for a in club_links]
        club_names = [t(a.get("title", "") or a.text) for a in club_links]
        injuries.append({
            "season":         t(tds[0].text),
            "injury_type":    t(tds[1].text),
            "start_date":     t(tds[2].text),
            "end_date":       t(tds[3].text),
            "matches_missed": t(tds[4].text) if len(tds) > 4 else None,
            "club":           club_names[0] if club_names else None,
            "club_tm_id":     club_ids[0] if club_ids else None,
        })
    cache_set(ck, injuries)
    return ok(injuries, "tm")


# ─────────────────────────────────────────
# 6. PLAYER SUSPENSIONS (nieuw)
# ─────────────────────────────────────────

@router.get("/player/{tm_id}/suspensions")
def player_suspensions(tm_id: str):
    ck = cache_key("tm", "suspensions_v22", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/x/sperrenhistorie/spieler/{tm_id}")
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
            "reason":            classify_suspension_reason(reason_raw),
            "reason_raw":        reason_raw,
        })
    cache_set(ck, suspensions)
    return ok(suspensions, "tm")


# ─────────────────────────────────────────
# 7. PLAYER NATIONAL TEAM STATS (nieuw)
# ─────────────────────────────────────────

@router.get("/player/{tm_id}/national-team")
def player_national_team(tm_id: str):
    ck = cache_key("tm", "national_v22", id=tm_id)
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
# v20 basis + shirt_number, market_value_currency
# ─────────────────────────────────────────

@router.get("/club/{tm_id}/squad")
def club_squad(tm_id: str):
    ck = cache_key("tm", "squad_v22", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/x/kader/verein/{tm_id}/saison_id/2024/plus/1")
    players = []
    for row in soup.select("table.items tbody tr.odd, table.items tbody tr.even"):
        name_a   = row.select_one("td.hauptlink a")
        mv_td    = row.select_one("td.rechts.hauptlink")
        nat      = [img["title"] for img in row.select("td.zentriert img.flaggenrahmen")]
        href     = name_a["href"] if name_a else None
        pid      = extract_id(href, "/spieler/")
        pos_td   = row.select_one("td.posrela table td")
        shirt_td = row.select_one("td.rn_nummer")
        mv_raw   = t(mv_td.text) if mv_td else ""
        mv       = clean_market_value(mv_raw)
        players.append({
            "player_tm_id":          pid,
            "name":                  clean_name(t(name_a.text)) if name_a else None,
            "shirt_number":          t(shirt_td.text) if shirt_td else None,
            "position":              t(pos_td.text) if pos_td else None,
            "nationality":           nat,
            "market_value":          mv["value"],
            "market_value_currency": mv["currency"],
        })
    cache_set(ck, players)
    return ok(players, "tm")


# ─────────────────────────────────────────
# 9. COMPETITION STANDINGS
# v20 ongewijzigd — werkte perfect
# ─────────────────────────────────────────

@router.get("/competition/{comp_id}/standings")
def standings(comp_id: str):
    lid = comp_id.upper()
    if lid not in COMP_SLUG:
        raise HTTPException(status_code=400, detail=f"Unsupported competition: {comp_id}")
    slug, code = COMP_SLUG[lid]

    ck = cache_key("tm", "standings_v22", comp=lid)
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
        gf_ga  = t(tds[7].text)
        table.append({
            "position":        int(tds[0].text.strip()),
            "club":            t(club_a.text) if club_a else None,
            "club_tm_id":      extract_id(href, "/verein/"),
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
# v20 basis (away_team fix bewezen) + home_score/away_score split, status, competition velden
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

    ck = cache_key("tm", "fixtures_v22", comp=lid, md=matchday)
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
        # v20 bewezen fix: filter op /verein/ in href
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
# 11. MATCH DETAILS (nieuw)
# Selectors: gebaseerd op felipeall game.py (bewezen werkend)
# ─────────────────────────────────────────

@router.get("/match/{game_id}")
def match_details(game_id: str):
    ck = cache_key("tm", "match_v22", id=game_id)
    cached = cache_get(ck, TTL_LIVE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/spielbericht/index/spielbericht/{game_id}")

    # ── Score — felipeall: sb-endstand ─────
    score_el  = soup.find(class_="sb-endstand")
    score_raw = t(score_el.get_text()) if score_el else None
    score_clean = re.sub(r"\s+", "", score_raw) if score_raw else None
    home_score = score_clean.split(":")[0] if score_clean and ":" in score_clean else None
    away_score = score_clean.split(":")[1] if score_clean and ":" in score_clean else None

    # ── Teams — felipeall: sb-team + /verein/ ─────
    club_ids, club_names = [], []
    seen = set()
    for team in soup.find_all(class_="sb-team"):
        a = team.find("a", href=lambda h: h and "/verein/" in h)
        if a:
            cid = extract_id(a["href"], "/verein/")
            if cid not in seen:
                seen.add(cid)
                club_ids.append(cid)
                img = team.find("img")
                club_names.append(img["alt"] if img and img.get("alt") else t(a.text))

    home_tm_id = club_ids[0] if club_ids else None
    away_tm_id = club_ids[1] if len(club_ids) > 1 else None
    home_name  = club_names[0] if club_names else None
    away_name  = club_names[1] if len(club_names) > 1 else None

    # ── Competition / round ─────
    comp_a = soup.select_one("a.sb-wettbewerb")

    # ── Info velden (stadion, referee, datum) ─────
    def find_label(keyword: str):
        el = soup.find(string=re.compile(keyword, re.IGNORECASE))
        if not el:
            return None
        parent = el.find_parent()
        if not parent:
            return None
        nxt = parent.find_next("a") or parent.find_next("span")
        return t(nxt.text) if nxt else None

    # Referee — felipeall bewezen
    referee = None
    ref_label = soup.find(string="Referee:")
    if ref_label:
        a = ref_label.find_parent().find_next("a")
        if a:
            referee = t(a.get_text())

    # Stadion, attendance uit header
    stadium    = find_label("Stadium|Stadion")
    attendance = find_label("Attendance|Zuschauer")

    meta = {
        "game_id":           game_id,
        "competition":       t(comp_a.text) if comp_a else None,
        "competition_tm_id": extract_id(comp_a["href"] if comp_a else None, "/wettbewerb/"),
        "season":            None,
        "round":             None,
        "kickoff_datetime":  None,
        "stadium":           stadium,
        "city":              None,
        "attendance":        attendance,
        "referee":           referee,
        "home_team":         home_name,
        "home_team_tm_id":   home_tm_id,
        "away_team":         away_name,
        "away_team_tm_id":   away_tm_id,
        "home_score":        home_score,
        "away_score":        away_score,
        "status":            parse_status(score_clean or ""),
    }

    # ── Goals — felipeall bewezen selectors ─────
    # sb-aktion-spielstand = score indicator = echte goal (niet kaart/wissel)
    goals = []
    for side_class, club_name, club_tm_id in [
        ("sb-aktion-heim", home_name, home_tm_id),
        ("sb-aktion-gast", away_name, away_tm_id),
    ]:
        for row in soup.find_all(class_=side_class):
            score_div = row.find(class_="sb-aktion-spielstand")
            if not score_div or not score_div.get_text(strip=True):
                continue
            try:
                links = row.find_all("a", href=lambda h: h and "leistungsdaten" in h)
                scorer_a  = links[0] if links else None
                assist_a  = links[1] if len(links) > 1 else None
                minute_el = row.find(class_="sb-sprite-uhr-klein") or row.find(class_=re.compile("uhr"))
                type_el   = row.find("span", class_=re.compile("sb-aktion-icon|icon"))
                type_text = t(type_el.get("title", "")) if type_el else ""
                goals.append({
                    "minute":       t(minute_el.text) if minute_el else None,
                    "scorer_name":  t(scorer_a.text) if scorer_a else None,
                    "scorer_tm_id": re.search(r"/spieler/(\d+)", scorer_a["href"]).group(1) if scorer_a else None,
                    "assist_name":  t(assist_a.text) if assist_a else None,
                    "assist_tm_id": re.search(r"/spieler/(\d+)", assist_a["href"]).group(1) if assist_a else None,
                    "assist_type":  _classify_assist(type_text),
                    "goal_type":    _classify_goal(type_text),
                    "club":         club_name,
                    "club_tm_id":   club_tm_id,
                })
            except Exception:
                continue

    # ── Cards ─────
    cards = []
    for side_class, club_name, club_tm_id in [
        ("sb-aktion-heim", home_name, home_tm_id),
        ("sb-aktion-gast", away_name, away_tm_id),
    ]:
        for row in soup.find_all(class_=side_class):
            # Kaarten hebben geen sb-aktion-spielstand maar wel een kaart icon
            card_icon = row.find("span", class_=re.compile("gelb|rot|karte|card"))
            if not card_icon:
                continue
            player_a  = row.find("a", href=lambda h: h and "/spieler/" in h)
            minute_el = row.find(class_="sb-sprite-uhr-klein") or row.find(class_=re.compile("uhr"))
            icon_title = t(card_icon.get("title", "")) or t(card_icon.get("class", [""])[0])
            cards.append({
                "minute":       t(minute_el.text) if minute_el else None,
                "player_name":  t(player_a.text) if player_a else None,
                "player_tm_id": re.search(r"/spieler/(\d+)", player_a["href"]).group(1) if player_a else None,
                "card_type":    _classify_card(icon_title),
                "club":         club_name,
                "club_tm_id":   club_tm_id,
            })

    # ── Lineups — felipeall: formation-player-container ─────
    all_players_el = soup.find_all(class_="formation-player-container")
    lineups = []
    for i, container in enumerate(all_players_el[:22]):
        a = container.find("a", href=lambda h: h and "/spieler/" in h)
        pid = re.search(r"/spieler/(\d+)", a["href"]).group(1) if a else None
        name = re.sub(r"^\d+", "", t(container.get_text())).strip()
        is_home = i < 11
        lineups.append({
            "player_name":  name,
            "player_tm_id": pid,
            "club_tm_id":   home_tm_id if is_home else away_tm_id,
            "position":     None,
            "shirt_number": None,
            "is_starting":  True,
            "is_captain":   False,
        })

    # Bench — felipeall: aufstellung-ersatzbank-box
    benches = soup.find_all(class_="aufstellung-ersatzbank-box")
    for bench_i, bench in enumerate(benches[:2]):
        is_home = bench_i == 0
        seen_ids = set()
        for a in bench.find_all("a", href=lambda h: h and "/spieler/" in h):
            pid = re.search(r"/spieler/(\d+)", a["href"]).group(1)
            if pid in seen_ids:
                continue
            seen_ids.add(pid)
            lineups.append({
                "player_name":  t(a.text),
                "player_tm_id": pid,
                "club_tm_id":   home_tm_id if is_home else away_tm_id,
                "position":     None,
                "shirt_number": None,
                "is_starting":  False,
                "is_captain":   False,
            })

    # ── Substitutions ─────
    substitutions = []
    for side_class, club_name, club_tm_id in [
        ("sb-aktion-heim", home_name, home_tm_id),
        ("sb-aktion-gast", away_name, away_tm_id),
    ]:
        for row in soup.find_all(class_=side_class):
            # Wissels hebben pijl-icon
            wechsel = row.find(class_=re.compile("wechsel|sub|arrow"))
            if not wechsel:
                continue
            player_links = row.find_all("a", href=lambda h: h and "/spieler/" in h)
            out_a = player_links[0] if len(player_links) > 0 else None
            in_a  = player_links[1] if len(player_links) > 1 else None
            minute_el = row.find(class_="sb-sprite-uhr-klein") or row.find(class_=re.compile("uhr"))
            substitutions.append({
                "minute":           t(minute_el.text) if minute_el else None,
                "player_out_name":  t(out_a.text) if out_a else None,
                "player_out_tm_id": re.search(r"/spieler/(\d+)", out_a["href"]).group(1) if out_a else None,
                "player_in_name":   t(in_a.text) if in_a else None,
                "player_in_tm_id":  re.search(r"/spieler/(\d+)", in_a["href"]).group(1) if in_a else None,
                "club":             club_name,
                "club_tm_id":       club_tm_id,
            })

    # ── Player match stats (afgeleid uit lineups + subs + goals + cards) ─────
    pm: dict[str, dict] = {}
    for p in lineups:
        pid = p["player_tm_id"]
        if not pid:
            continue
        pm[pid] = {
            "player_tm_id": pid, "club_tm_id": p["club_tm_id"],
            "start_min": 0 if p["is_starting"] else None,
            "end_min":   90 if p["is_starting"] else None,
            "goals": 0, "assists": 0, "yellow_cards": 0, "red_cards": 0,
        }
    for sub in substitutions:
        mn = parse_minute(sub["minute"])
        if sub["player_out_tm_id"] and sub["player_out_tm_id"] in pm:
            pm[sub["player_out_tm_id"]]["end_min"] = mn
        if sub["player_in_tm_id"]:
            if sub["player_in_tm_id"] not in pm:
                pm[sub["player_in_tm_id"]] = {
                    "player_tm_id": sub["player_in_tm_id"], "club_tm_id": sub["club_tm_id"],
                    "start_min": mn, "end_min": 90,
                    "goals": 0, "assists": 0, "yellow_cards": 0, "red_cards": 0,
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
                pm[pid]["end_min"] = parse_minute(c["minute"])
            else:
                pm[pid]["yellow_cards"] += 1

    player_match_stats = []
    for pid, d in pm.items():
        s, e = d["start_min"], d["end_min"]
        player_match_stats.append({
            "player_tm_id":   pid,
            "club_tm_id":     d["club_tm_id"],
            "minutes_played": (e - s) if (s is not None and e is not None) else None,
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


def _classify_goal(raw: str) -> str:
    rl = raw.lower()
    if "penalty" in rl or "pen" in rl:
        return "penalty"
    if "own" in rl:
        return "own_goal"
    if "header" in rl:
        return "header"
    if "free kick" in rl or "freekick" in rl:
        return "free_kick"
    return "goal"

def _classify_assist(raw: str) -> str:
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

def _classify_card(raw: str) -> str:
    rl = raw.lower()
    if "second" in rl or "2nd" in rl or "gelb-rot" in rl:
        return "second_yellow"
    if "red" in rl or "rot" in rl:
        return "red"
    return "yellow"


# ─────────────────────────────────────────
# 12. COMPETITIONS METADATA (nieuw — statisch)
# ─────────────────────────────────────────

@router.get("/competitions")
def competitions():
    return ok(COMPETITIONS_METADATA, "tm")


# ─────────────────────────────────────────
# 13. CLUBS METADATA (nieuw)
# ─────────────────────────────────────────

@router.get("/clubs")
def clubs(comp_id: str = Query(..., description="Competition TM ID bijv. GB1")):
    lid = comp_id.upper()
    if lid not in COMP_SLUG:
        raise HTTPException(status_code=400, detail=f"Unsupported competition: {comp_id}")
    slug, code = COMP_SLUG[lid]

    ck = cache_key("tm", "clubs_v22", comp=lid)
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
        cid = extract_id(club_a.get("href", ""), "/verein/")
        if not cid or cid in seen:
            continue
        seen.add(cid)
        clubs_out.append({
            "club_tm_id": cid,
            "club_name":  t(club_a.text),
            "country":    None,
            "stadium":    None,
            "founded":    None,
        })
    cache_set(ck, clubs_out)
    return ok(clubs_out, "tm")
