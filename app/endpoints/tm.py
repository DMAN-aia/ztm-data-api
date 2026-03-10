import re
import time
import random
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from fastapi import APIRouter, HTTPException, Query

from app.utils.common import ok, cache_key, cache_get, cache_set

router = APIRouter()

TM_BASE = "https://www.transfermarkt.com"
TM_CEAPI = "https://www.transfermarkt.com/ceapi"

TTL_PROFILE = 86400
TTL_LIVE = 3600
TTL_MV = 43200

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html",
    "Connection": "keep-alive",
}

session = requests.Session()
session.headers.update(HEADERS)

# ─────────────────────────────────────────
# COMP SLUG MAP
# ─────────────────────────────────────────

COMP_SLUG = {
    "GB1":  "premier-league",
    "GB2":  "championship",
    "L1":   "1-bundesliga",
    "L2":   "2-bundesliga",
    "L3":   "3-liga",
    "NL1":  "eredivisie",
    "NL2":  "eerste-divisie",
    "BE1":  "jupiler-pro-league",
    "BE2":  "challenger-pro-league",
    "SC1":  "scottish-premiership",
    "PO1":  "primeira-liga",
    "FR1":  "ligue-1",
    "FR2":  "ligue-2",
    "IT1":  "serie-a",
    "IT2":  "serie-b",
    "DK1":  "superliga",
    "C1":   "super-league",
    "A1":   "bundesliga",
    "SE1":  "allsvenskan",
    "NO1":  "eliteserien",
    "PL1":  "ekstraklasa",
    "TS1":  "czech-football-league",
    "SK1":  "fortuna-liga",
    "UNG1": "nb-i",
    "ES1":  "laliga",
    "ES2":  "laliga2",
    "CL":   "champions-league",
    "EL":   "europa-league",
    "ECL":  "conference-league",
    "MLS":  "major-league-soccer",
    "USAM": "usl-championship",
    "MLP":  "mls-next-pro",
    "JP1":  "j1-league",
    "KR1":  "k-league-1",
    "TH1":  "thai-league",
    "VN1":  "v-league-1",
    "MY1":  "super-league",
    "SA":   "saudi-professional-league",
    "AL":   "a-league",
    "TR1":  "super-lig",
}

# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

def fetch(url: str) -> BeautifulSoup:
    time.sleep(random.uniform(1.4, 2.8))
    try:
        r = session.get(url, timeout=25)
    except requests.exceptions.RequestException:
        raise HTTPException(status_code=500, detail="Transfermarkt request failed")
    if r.status_code == 403:
        time.sleep(random.uniform(5, 8))
        r = session.get(url, timeout=25)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")


def fetch_json(url: str):
    time.sleep(random.uniform(1.0, 2.0))
    r = session.get(url, timeout=25)
    if r.status_code == 403:
        time.sleep(random.uniform(5, 8))
        r = session.get(url, timeout=25)
    r.raise_for_status()
    return r.json()


def extract_id(href: str, segment: str):
    if not href or segment not in href:
        return None
    try:
        return href.split(segment)[1].split("/")[0]
    except Exception:
        return None


def clean_name(name):
    if not name:
        return None
    name = re.sub(r"#\d+", "", name)
    name = re.sub(r"\s+", " ", name)
    return name.strip()


def parse_int(v):
    if not v or v == "-":
        return None
    try:
        return int(re.sub(r"[^\d]", "", str(v)))
    except Exception:
        return None


def clean_market_value(v: str):
    if not v or v.strip() in ("-", ""):
        return None
    v = v.strip()
    m = re.search(r"([\d,.]+)\s*(k|m|bn)?", v, re.IGNORECASE)
    if not m:
        return None
    num = float(m.group(1).replace(",", "."))
    suffix = (m.group(2) or "").lower()
    if suffix == "k":
        num *= 1_000
    elif suffix == "m":
        num *= 1_000_000
    elif suffix == "bn":
        num *= 1_000_000_000
    return int(num)


def now_iso():
    return datetime.utcnow().isoformat() + "Z"


def club_name_from_a(tag):
    if not tag:
        return None
    title = tag.get("title") or tag.get("alt")
    if title:
        return title.strip()
    return tag.text.strip() or None


def classify_transfer_type(fee_text: str) -> str:
    if not fee_text:
        return "unknown"
    fee_lower = fee_text.lower()
    if "loan" in fee_lower or "leih" in fee_lower:
        return "loan"
    if "end of loan" in fee_lower or "loan back" in fee_lower:
        return "loan_end"
    if "free" in fee_lower or "ablösefrei" in fee_lower:
        return "free_transfer"
    return "transfer"

# ─────────────────────────────────────────
# EXISTING ENDPOINTS
# ─────────────────────────────────────────

@router.get("/player/{tm_id}")
def player_profile(tm_id: str):
    ck = cache_key("tm", "profile_final", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/-/profil/spieler/{tm_id}")

    name_tag = soup.find("h1")
    club_tag = soup.select_one("span.data-header__club a")
    nationalities = list(dict.fromkeys(
        img.get("title")
        for img in soup.select("img.flaggenrahmen")
        if img.get("title")
    ))

    data = {
        "tm_id": tm_id,
        "name": clean_name(name_tag.text if name_tag else None),
        "nationalities": nationalities,
        "current_club": club_tag.text.strip() if club_tag else None,
        "club_tm_id": extract_id(club_tag["href"], "/verein/") if club_tag else None,
        "last_updated": now_iso(),
    }

    cache_set(ck, data)
    return ok(data, "tm")


@router.get("/player/{tm_id}/stats")
def player_stats(tm_id: str, season_id: str = Query("2025")):
    soup = fetch(f"{TM_BASE}/-/leistungsdaten/spieler/{tm_id}/plus/0?saison={season_id}")
    headers = [th.text.strip().lower() for th in soup.select("table.items thead th")]
    rows = []

    for tr in soup.select("table.items tbody tr"):
        tds = [td.text.strip() for td in tr.find_all("td")]
        if len(tds) < 5:
            continue
        comp_a = tr.select_one("td:nth-child(2) a")
        row = {
            "season": season_id,
            "competition": comp_a.text.strip() if comp_a else None,
            "competition_tm_id": extract_id(comp_a["href"], "/wettbewerb/") if comp_a else None,
        }
        for i, h in enumerate(headers):
            if i >= len(tds):
                continue
            v = parse_int(tds[i])
            if "goals" in h:
                row["goals"] = v
            if "assists" in h:
                row["assists"] = v
            if "yellow" in h:
                row["yellow_cards"] = v
            if "red" in h:
                row["red_cards"] = v
            if "minutes" in h:
                row["minutes"] = v
            if "appearances" in h:
                row["appearances"] = v
        rows.append(row)

    return ok(rows, "tm")


@router.get("/player/{tm_id}/transfers")
def player_transfers(tm_id: str):
    ck = cache_key("tm", "player_transfers", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    raw = fetch_json(f"{TM_CEAPI}/transferHistory/list/{tm_id}")
    rows = []

    for item in raw.get("transfers", []):
        fee_text = item.get("fee", "")
        rows.append({
            "season": item.get("season"),
            "date": item.get("date"),
            "from_club": item.get("from", {}).get("clubName"),
            "from_club_tm_id": item.get("from", {}).get("id"),
            "to_club": item.get("to", {}).get("clubName"),
            "to_club_tm_id": item.get("to", {}).get("id"),
            "fee_text": fee_text,
            "fee_value": clean_market_value(fee_text),
            "transfer_type": classify_transfer_type(fee_text),
        })

    cache_set(ck, rows)
    return ok(rows, "tm")


@router.get("/player/{tm_id}/market-value-history")
def player_market_value_history(tm_id: str):
    ck = cache_key("tm", "mv_history", id=tm_id)
    cached = cache_get(ck, TTL_MV)
    if cached:
        return ok(cached, "tm", cached=True)

    raw = fetch_json(f"{TM_CEAPI}/marketValueDevelopment/graph/{tm_id}")
    rows = []

    for item in raw.get("list", []):
        rows.append({
            "date": item.get("datum_mw") or item.get("date"),
            "value": clean_market_value(item.get("mw") or item.get("value", "")),
            "club": item.get("verein") or item.get("club"),
            "age": item.get("age"),
        })

    cache_set(ck, rows)
    return ok(rows, "tm")


@router.get("/player/{tm_id}/injuries")
def player_injuries(tm_id: str):
    ck = cache_key("tm", "injuries", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/-/verletzungen/spieler/{tm_id}")
    rows = []

    for tr in soup.select("table.items tbody tr"):
        tds = [td.text.strip() for td in tr.find_all("td")]
        if len(tds) < 5:
            continue
        rows.append({
            "season": tds[0] if len(tds) > 0 else None,
            "injury": tds[1] if len(tds) > 1 else None,
            "from_date": tds[2] if len(tds) > 2 else None,
            "until_date": tds[3] if len(tds) > 3 else None,
            "days": parse_int(tds[4]) if len(tds) > 4 else None,
            "games_missed": parse_int(tds[5]) if len(tds) > 5 else None,
        })

    cache_set(ck, rows)
    return ok(rows, "tm")


@router.get("/player/{tm_id}/national-team")
def player_national_team(tm_id: str):
    ck = cache_key("tm", "national_team", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/-/nationalmannschaft/spieler/{tm_id}")
    rows = []

    for tr in soup.select("table.items tbody tr"):
        tds = [td.text.strip() for td in tr.find_all("td")]
        if len(tds) < 3:
            continue
        rows.append({
            "national_team": tds[1] if len(tds) > 1 else None,
            "debut": tds[2] if len(tds) > 2 else None,
            "caps": parse_int(tds[3]) if len(tds) > 3 else None,
            "goals": parse_int(tds[4]) if len(tds) > 4 else None,
        })

    cache_set(ck, rows)
    return ok(rows, "tm")


@router.get("/club/{tm_id}/squad")
def club_squad(tm_id: str):
    ck = cache_key("tm", "squad", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/-/kader/verein/{tm_id}/saison_id/2025/plus/1")
    players = []

    for tr in soup.select("table.items tbody tr"):
        pid_a = tr.select_one("td.hauptlink a[href*='/spieler/']")
        if not pid_a:
            continue
        pid = extract_id(pid_a.get("href", ""), "/spieler/")
        name = clean_name(pid_a.text)

        pos_td = tr.select_one("td.posrela table td:last-child")
        pos = pos_td.text.strip() if pos_td else None

        nat_imgs = tr.select("img.flaggenrahmen")
        nats = list(dict.fromkeys(img.get("title") for img in nat_imgs if img.get("title")))

        mv_td = tr.select_one("td.rechts.hauptlink")
        mv_text = mv_td.text.strip() if mv_td else None

        age_td = tr.select_one("td.zentriert")
        age = parse_int(age_td.text.strip()) if age_td else None

        players.append({
            "tm_id": pid,
            "name": name,
            "position": pos,
            "nationalities": nats,
            "age": age,
            "market_value": clean_market_value(mv_text),
            "market_value_text": mv_text,
        })

    cache_set(ck, players)
    return ok(players, "tm")


@router.get("/competition/{comp_id}/standings")
def competition_standings(comp_id: str, season_id: str = Query("2024")):
    ck = cache_key("tm", "standings", comp=comp_id, season=season_id)
    cached = cache_get(ck, TTL_LIVE)
    if cached:
        return ok(cached, "tm", cached=True)

    slug = COMP_SLUG.get(comp_id, comp_id.lower())
    soup = fetch(f"{TM_BASE}/{slug}/tabelle/wettbewerb/{comp_id}/saison_id/{season_id}")
    rows = []

    for tr in soup.select("table.items tbody tr"):
        tds = [td.text.strip() for td in tr.find_all("td")]
        if len(tds) < 8:
            continue
        club_a = tr.select_one("td.no-border-links.hauptlink a")
        rows.append({
            "position": parse_int(tds[0]),
            "club": club_a.text.strip() if club_a else tds[1],
            "club_tm_id": extract_id(club_a["href"], "/verein/") if club_a else None,
            "played": parse_int(tds[2]),
            "won": parse_int(tds[3]),
            "drawn": parse_int(tds[4]),
            "lost": parse_int(tds[5]),
            "goals_for": parse_int(tds[6].split(":")[0]) if ":" in tds[6] else parse_int(tds[6]),
            "goals_against": parse_int(tds[6].split(":")[1]) if ":" in tds[6] else None,
            "goal_diff": parse_int(tds[7]),
            "points": parse_int(tds[8]) if len(tds) > 8 else None,
        })

    return ok(rows, "tm")


@router.get("/competition/{comp_id}/fixtures")
def competition_fixtures(comp_id: str, season_id: str = Query("2024")):
    ck = cache_key("tm", "fixtures", comp=comp_id, season=season_id)
    cached = cache_get(ck, TTL_LIVE)
    if cached:
        return ok(cached, "tm", cached=True)

    slug = COMP_SLUG.get(comp_id, comp_id.lower())
    soup = fetch(f"{TM_BASE}/{slug}/gesamtspielplan/wettbewerb/{comp_id}/saison_id/{season_id}")
    rows = []

    for tr in soup.select("table.spielplandatum tbody tr, table.items tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue

        home_a = tr.select_one("td.rechts a[href*='/verein/']")
        away_a = tr.select_one("td.links a[href*='/verein/']")
        result_a = tr.select_one("td.ergebnis-link a, a.ergebnis-link")

        result_text = result_a.text.strip() if result_a else None
        game_id = extract_id(result_a["href"], "/spielbericht/") if result_a else None

        date_td = tr.select_one("td.zentriert")
        date_text = date_td.text.strip() if date_td else None

        rows.append({
            "game_id": game_id,
            "date": date_text,
            "home_club": home_a.text.strip() if home_a else None,
            "home_club_tm_id": extract_id(home_a["href"], "/verein/") if home_a else None,
            "away_club": away_a.text.strip() if away_a else None,
            "away_club_tm_id": extract_id(away_a["href"], "/verein/") if away_a else None,
            "result": result_text,
        })

    cache_set(ck, rows)
    return ok(rows, "tm")


@router.get("/competitions")
def competitions():
    return ok([
        {"comp_id": k, "name": v, "slug": COMP_SLUG.get(k)}
        for k, v in {
            "GB1": "Premier League", "GB2": "Championship",
            "L1": "Bundesliga", "L2": "2. Bundesliga", "L3": "3. Liga",
            "NL1": "Eredivisie", "NL2": "Eerste Divisie",
            "BE1": "Belgian Pro League", "BE2": "Challenger Pro League",
            "SC1": "Scottish Premiership",
            "PO1": "Primeira Liga",
            "FR1": "Ligue 1", "FR2": "Ligue 2",
            "IT1": "Serie A", "IT2": "Serie B",
            "DK1": "Danish Superliga", "C1": "Swiss Super League",
            "A1": "Austrian Bundesliga", "SE1": "Allsvenskan",
            "NO1": "Eliteserien", "PL1": "Ekstraklasa",
            "TS1": "Czech First League", "SK1": "Slovak Super Liga",
            "UNG1": "Hungarian NB I",
            "ES1": "La Liga", "ES2": "La Liga 2",
            "CL": "UEFA Champions League", "EL": "UEFA Europa League",
            "ECL": "UEFA Conference League",
            "MLS": "Major League Soccer", "USAM": "USL Championship",
            "MLP": "MLS Next Pro",
            "JP1": "J1 League", "KR1": "K League 1",
            "TH1": "Thai League", "VN1": "V.League 1",
            "MY1": "Super League Malaysia", "SA": "Saudi Pro League",
            "AL": "A-League Men", "TR1": "Süper Lig",
        }.items()
    ], "tm")


@router.get("/clubs")
def clubs(comp_id: str = Query(...)):
    ck = cache_key("tm", "clubs", comp=comp_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    slug = COMP_SLUG.get(comp_id, comp_id.lower())
    soup = fetch(f"{TM_BASE}/{slug}/startseite/wettbewerb/{comp_id}")
    clubs_list = []
    seen = set()

    for a in soup.select("td.hauptlink a[href*='/startseite/verein/']"):
        href = a.get("href", "")
        club_tm_id = extract_id(href, "/verein/")
        if not club_tm_id or club_tm_id in seen:
            continue
        seen.add(club_tm_id)
        clubs_list.append({
            "club_tm_id": club_tm_id,
            "name": a.text.strip(),
        })

    cache_set(ck, clubs_list)
    return ok(clubs_list, "tm")


@router.get("/match/{game_id}")
def match_detail(game_id: str):
    ck = cache_key("tm", "match", id=game_id)
    cached = cache_get(ck, TTL_LIVE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/-/spielbericht/index/spielbericht/{game_id}")

    home_tag = soup.select_one("div.sb-team.sb-heim a[href*='/verein/']")
    away_tag = soup.select_one("div.sb-team.sb-gast a[href*='/verein/']")
    score_tag = soup.select_one("div.sb-ergebnis span.sb-endstand")

    data = {
        "game_id": game_id,
        "home_club": home_tag.text.strip() if home_tag else None,
        "home_club_tm_id": extract_id(home_tag["href"], "/verein/") if home_tag else None,
        "away_club": away_tag.text.strip() if away_tag else None,
        "away_club_tm_id": extract_id(away_tag["href"], "/verein/") if away_tag else None,
        "score": score_tag.text.strip() if score_tag else None,
        "last_updated": now_iso(),
    }

    cache_set(ck, data)
    return ok(data, "tm")


# ─────────────────────────────────────────
# NEW IN v38: CLUB ENDPOINTS
# ─────────────────────────────────────────

@router.get("/club/{tm_id}/profile")
def club_profile(tm_id: str):
    """Clubprofiel: naam, stadion, eigenaar, stichting, kleuren, marktwaarde."""
    ck = cache_key("tm", "club_profile", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/-/datenfakten/verein/{tm_id}")

    name_tag = soup.select_one("h1.data-header__headline-wrapper") or soup.find("h1")

    # Data items in the info table
    info = {}
    for tr in soup.select("table.profilheader tr"):
        tds = tr.find_all("td")
        if len(tds) == 2:
            label = tds[0].text.strip().rstrip(":").lower()
            value = tds[1].text.strip()
            info[label] = value

    # Market value from header
    mv_tag = soup.select_one("a.data-header__market-value-wrapper")
    mv_text = mv_tag.text.strip() if mv_tag else None

    data = {
        "tm_id": tm_id,
        "name": clean_name(name_tag.text if name_tag else None),
        "stadium": info.get("stadium") or info.get("stadion"),
        "stadium_capacity": parse_int(info.get("seats") or info.get("capacity") or info.get("plätze")),
        "founded": info.get("founded") or info.get("gründung") or info.get("founded:"),
        "address": info.get("address") or info.get("adresse"),
        "website": info.get("website"),
        "total_market_value": clean_market_value(mv_text),
        "total_market_value_text": mv_text,
        "last_updated": now_iso(),
    }

    cache_set(ck, data)
    return ok(data, "tm")


@router.get("/club/{tm_id}/transfers")
def club_transfers(tm_id: str, season_id: str = Query("2024")):
    """Transfers in en uit voor een club per seizoen/transferwindow."""
    ck = cache_key("tm", "club_transfers", id=tm_id, season=season_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/-/transfers/verein/{tm_id}/saison_id/{season_id}")

    def parse_transfer_table(table):
        rows = []
        if not table:
            return rows
        for tr in table.select("tbody tr"):
            tds = tr.find_all("td")
            if len(tds) < 5:
                continue

            player_a = tr.select_one("td.hauptlink a[href*='/spieler/']")
            club_a = tr.select_one("td.no-border-links a[href*='/verein/']")

            fee_td = tds[-1]
            fee_text = fee_td.text.strip()

            rows.append({
                "player_name": clean_name(player_a.text) if player_a else None,
                "player_tm_id": extract_id(player_a["href"], "/spieler/") if player_a else None,
                "club": club_a.text.strip() if club_a else None,
                "club_tm_id": extract_id(club_a["href"], "/verein/") if club_a else None,
                "fee_text": fee_text,
                "fee_value": clean_market_value(fee_text),
                "transfer_type": classify_transfer_type(fee_text),
            })
        return rows

    # TM shows arrivals and departures in separate boxes
    boxes = soup.select("div.box")
    arrivals_table = None
    departures_table = None

    for box in boxes:
        header = box.select_one("h2.content-box-headline")
        if not header:
            continue
        header_text = header.text.strip().lower()
        table = box.select_one("table.items")
        if "arrival" in header_text or "zugänge" in header_text or "ins" in header_text:
            arrivals_table = table
        elif "departure" in header_text or "abgänge" in header_text or "out" in header_text:
            departures_table = table

    data = {
        "tm_id": tm_id,
        "season_id": season_id,
        "arrivals": parse_transfer_table(arrivals_table),
        "departures": parse_transfer_table(departures_table),
    }

    cache_set(ck, data)
    return ok(data, "tm")


@router.get("/club/{tm_id}/fixtures")
def club_fixtures(tm_id: str):
    """Aankomende wedstrijden van een club."""
    ck = cache_key("tm", "club_fixtures", id=tm_id)
    cached = cache_get(ck, TTL_LIVE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/-/termine/verein/{tm_id}")
    rows = []

    for tr in soup.select("table.items tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue

        date_td = tds[0]
        comp_a = tr.select_one("td a[href*='/wettbewerb/']")
        home_a = tr.select_one("td.rechts a[href*='/verein/']")
        away_a = tr.select_one("td.links a[href*='/verein/']")
        result_a = tr.select_one("a.ergebnis-link, td.ergebnis-link a")

        game_id = extract_id(result_a["href"], "/spielbericht/") if result_a else None
        result_text = result_a.text.strip() if result_a else None

        rows.append({
            "game_id": game_id,
            "date": date_td.text.strip(),
            "competition": comp_a.text.strip() if comp_a else None,
            "competition_tm_id": extract_id(comp_a["href"], "/wettbewerb/") if comp_a else None,
            "home_club": home_a.text.strip() if home_a else None,
            "home_club_tm_id": extract_id(home_a["href"], "/verein/") if home_a else None,
            "away_club": away_a.text.strip() if away_a else None,
            "away_club_tm_id": extract_id(away_a["href"], "/verein/") if away_a else None,
            "result": result_text,
        })

    cache_set(ck, rows)
    return ok(rows, "tm")


@router.get("/club/{tm_id}/results")
def club_results(tm_id: str, limit: int = Query(10, ge=1, le=50)):
    """Recente uitslagen van een club (standaard laatste 10)."""
    ck = cache_key("tm", "club_results", id=tm_id, limit=limit)
    cached = cache_get(ck, TTL_LIVE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/-/spielplan/verein/{tm_id}/last/{limit}")
    rows = []

    for tr in soup.select("table.items tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue

        date_td = tds[0]
        comp_a = tr.select_one("td a[href*='/wettbewerb/']")
        home_a = tr.select_one("td.rechts a[href*='/verein/']")
        away_a = tr.select_one("td.links a[href*='/verein/']")
        result_a = tr.select_one("a.ergebnis-link, td.ergebnis-link a")

        game_id = extract_id(result_a["href"], "/spielbericht/") if result_a else None
        result_text = result_a.text.strip() if result_a else None

        rows.append({
            "game_id": game_id,
            "date": date_td.text.strip(),
            "competition": comp_a.text.strip() if comp_a else None,
            "competition_tm_id": extract_id(comp_a["href"], "/wettbewerb/") if comp_a else None,
            "home_club": home_a.text.strip() if home_a else None,
            "home_club_tm_id": extract_id(home_a["href"], "/verein/") if home_a else None,
            "away_club": away_a.text.strip() if away_a else None,
            "away_club_tm_id": extract_id(away_a["href"], "/verein/") if away_a else None,
            "result": result_text,
        })

    cache_set(ck, rows)
    return ok(rows, "tm")


# ─────────────────────────────────────────
# NEW IN v38: COMPETITION TOP SCORERS
# ─────────────────────────────────────────

@router.get("/competition/{comp_id}/top-scorers")
def competition_top_scorers(comp_id: str, season_id: str = Query("2024")):
    """Topscorerslijst voor een competitie."""
    ck = cache_key("tm", "top_scorers", comp=comp_id, season=season_id)
    cached = cache_get(ck, TTL_LIVE)
    if cached:
        return ok(cached, "tm", cached=True)

    slug = COMP_SLUG.get(comp_id, comp_id.lower())
    soup = fetch(f"{TM_BASE}/{slug}/torjaeger/wettbewerb/{comp_id}/saison_id/{season_id}")
    rows = []

    for tr in soup.select("table.items tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue

        rank_td = tds[0]
        player_a = tr.select_one("td.hauptlink a[href*='/spieler/']")
        club_a = tr.select_one("td.no-border-links a[href*='/verein/']")

        nat_img = tr.select_one("img.flaggenrahmen")
        nationality = nat_img.get("title") if nat_img else None

        goals_td = tr.select_one("td.zentriert.hauptlink") or tds[-1]

        rows.append({
            "rank": parse_int(rank_td.text.strip()),
            "player_name": clean_name(player_a.text) if player_a else None,
            "player_tm_id": extract_id(player_a["href"], "/spieler/") if player_a else None,
            "nationality": nationality,
            "club": club_a.text.strip() if club_a else None,
            "club_tm_id": extract_id(club_a["href"], "/verein/") if club_a else None,
            "goals": parse_int(goals_td.text.strip()),
        })

    cache_set(ck, rows)
    return ok(rows, "tm")
