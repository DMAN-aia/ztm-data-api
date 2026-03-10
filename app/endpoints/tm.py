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

# Nationality filter for Asian diaspora detection
ASIAN_NATIONALITIES = {"Japan", "South Korea", "Vietnam", "Indonesia", "Thailand", "Philippines", "Malaysia"}

# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

def fetch(url: str) -> BeautifulSoup:
    time.sleep(random.uniform(1.4, 2.8))
    try:
        r = session.get(url, timeout=25)
    except requests.exceptions.RequestException:
        raise HTTPException(status_code=500, detail="Transfermarkt request failed")
    if r.status_code in (403, 429):
        time.sleep(random.uniform(5, 10))
        try:
            r = session.get(url, timeout=25)
        except requests.exceptions.RequestException:
            raise HTTPException(status_code=500, detail="Transfermarkt request failed after retry")
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")


def fetch_json(url: str):
    time.sleep(random.uniform(1.0, 2.0))
    r = session.get(url, timeout=25)
    if r.status_code in (403, 429):
        time.sleep(random.uniform(5, 10))
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
    if not v or str(v).strip() in ("-", ""):
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


def parse_form(soup) -> list:
    """Parse recent form from club page (W/D/L last 5)."""
    form = []
    for span in soup.select("div.data-header__details span.data-header__content a"):
        text = span.text.strip().upper()
        if text in ("W", "D", "L"):
            form.append(text)
    return form[-5:] if form else []

# ─────────────────────────────────────────
# PLAYER ENDPOINTS
# ─────────────────────────────────────────

@router.get("/player/{tm_id}")
def player_profile(tm_id: str):
    """Uitgebreid spelerprofiel: naam, positie, voet, lengte, geboortedatum, contract, marktwaarde."""
    ck = cache_key("tm", "profile_v39", id=tm_id)
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

    # Info table rows
    info = {}
    for tr in soup.select("table.auflistung tr"):
        tds = tr.find_all("td")
        if len(tds) == 2:
            label = tds[0].text.strip().rstrip(":").lower()
            value = tds[1].text.strip()
            info[label] = value

    # Market value from header
    mv_tag = soup.select_one("a.data-header__market-value-wrapper")
    mv_text = mv_tag.text.strip() if mv_tag else None

    # Jersey number
    shirt_tag = soup.select_one("span.data-header__shirt-number")
    shirt = shirt_tag.text.strip().replace("#", "") if shirt_tag else None

    # Position from header
    pos_tag = soup.select_one("dd.detail-position__position") or soup.select_one("span.data-header__label")

    # DOB from info table — multiple possible labels
    dob = (
        info.get("date of birth")
        or info.get("geburtsdatum")
        or info.get("dob")
    )

    # Birthplace
    birthplace = (
        info.get("place of birth")
        or info.get("geburtsort")
    )

    # Height
    height_raw = info.get("height") or info.get("größe")
    height_cm = None
    if height_raw:
        m = re.search(r"(\d[\d,\.]+)", height_raw)
        if m:
            height_cm = int(re.sub(r"[^\d]", "", m.group(1))[:3])

    # Foot
    foot = info.get("foot") or info.get("fuß")

    # Contract
    contract_until = info.get("contract expires") or info.get("vertrag bis")

    # Main position + other positions
    main_position = info.get("position") or info.get("hauptposition")
    other_positions_td = soup.select("td.nebenposition")
    other_positions = [td.text.strip() for td in other_positions_td if td.text.strip()]

    data = {
        "tm_id": tm_id,
        "name": clean_name(name_tag.text if name_tag else None),
        "shirt_number": shirt,
        "nationalities": nationalities,
        "date_of_birth": dob,
        "place_of_birth": birthplace,
        "age": parse_int(info.get("age") or info.get("alter")),
        "height_cm": height_cm,
        "foot": foot,
        "main_position": main_position,
        "other_positions": other_positions,
        "current_club": club_tag.text.strip() if club_tag else None,
        "club_tm_id": extract_id(club_tag["href"], "/verein/") if club_tag else None,
        "contract_until": contract_until,
        "market_value": clean_market_value(mv_text),
        "market_value_text": mv_text,
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


@router.get("/player/{tm_id}/suspensions")
def player_suspensions(tm_id: str):
    ck = cache_key("tm", "suspensions", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/-/sperren/spieler/{tm_id}")
    rows = []

    for tr in soup.select("table.items tbody tr"):
        tds = [td.text.strip() for td in tr.find_all("td")]
        if len(tds) < 4:
            continue
        comp_a = tr.select_one("td a[href*='/wettbewerb/']")
        rows.append({
            "competition": comp_a.text.strip() if comp_a else tds[1] if len(tds) > 1 else None,
            "competition_tm_id": extract_id(comp_a["href"], "/wettbewerb/") if comp_a else None,
            "reason": tds[2] if len(tds) > 2 else None,
            "matches_suspended": parse_int(tds[3]) if len(tds) > 3 else None,
            "from_date": tds[4] if len(tds) > 4 else None,
            "until_date": tds[5] if len(tds) > 5 else None,
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


@router.get("/player/{tm_id}/achievements")
def player_achievements(tm_id: str):
    """Trofeeën en individuele awards van een speler."""
    ck = cache_key("tm", "achievements", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/-/erfolge/spieler/{tm_id}")
    rows = []

    for box in soup.select("div.erfolg_box, div.box"):
        title_tag = box.select_one("h2, h3, .box-header")
        if not title_tag:
            continue
        category = title_tag.text.strip()

        for li in box.select("li, tr"):
            text = li.text.strip()
            if not text or text == category:
                continue
            # Try to extract year and competition name
            year_m = re.search(r"\b(19|20)\d{2}\b", text)
            rows.append({
                "category": category,
                "title": text,
                "year": year_m.group(0) if year_m else None,
            })

    cache_set(ck, rows)
    return ok(rows, "tm")


@router.get("/player/{tm_id}/rumours")
def player_rumours(tm_id: str):
    """Transfergeruchten voor een speler."""
    ck = cache_key("tm", "rumours", id=tm_id)
    cached = cache_get(ck, TTL_LIVE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/-/geruechte/spieler/{tm_id}")
    rows = []

    for tr in soup.select("table.items tbody tr"):
        tds = [td.text.strip() for td in tr.find_all("td")]
        if len(tds) < 3:
            continue
        club_a = tr.select_one("td.hauptlink a[href*='/verein/']")
        source_a = tr.select_one("td a[href^='http']")
        rows.append({
            "date": tds[0] if tds else None,
            "club": club_a.text.strip() if club_a else None,
            "club_tm_id": extract_id(club_a["href"], "/verein/") if club_a else None,
            "probability": tds[2] if len(tds) > 2 else None,
            "fee_estimate": tds[3] if len(tds) > 3 else None,
            "source": source_a.get("href") if source_a else None,
        })

    cache_set(ck, rows)
    return ok(rows, "tm")


@router.get("/player/{tm_id}/similar-players")
def player_similar(tm_id: str):
    """Vergelijkbare spelers op basis van TM profiel."""
    ck = cache_key("tm", "similar", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/-/profil/spieler/{tm_id}")
    rows = []

    for a in soup.select("div.similar-players a[href*='/spieler/'], div.vergleichsspieler a[href*='/spieler/']"):
        pid = extract_id(a.get("href", ""), "/spieler/")
        name = clean_name(a.text)
        if pid and name:
            rows.append({"tm_id": pid, "name": name})

    cache_set(ck, rows)
    return ok(rows, "tm")


@router.get("/player/{tm_id}/jersey-numbers")
def player_jersey_numbers(tm_id: str):
    """Alle rugnummers die een speler bij verschillende clubs heeft gedragen."""
    ck = cache_key("tm", "jersey_numbers", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/-/rueckennummern/spieler/{tm_id}")
    rows = []

    for tr in soup.select("table.items tbody tr"):
        tds = [td.text.strip() for td in tr.find_all("td")]
        if len(tds) < 2:
            continue
        club_a = tr.select_one("td a[href*='/verein/']")
        rows.append({
            "club": club_a.text.strip() if club_a else tds[0],
            "club_tm_id": extract_id(club_a["href"], "/verein/") if club_a else None,
            "jersey_number": tds[-1],
        })

    cache_set(ck, rows)
    return ok(rows, "tm")


@router.get("/player/{tm_id}/timeline")
def player_timeline(tm_id: str):
    """Gecombineerde tijdlijn: transfers + blessures + marktwaarde in één response."""
    ck = cache_key("tm", "timeline", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    # Transfers via CEAPI
    transfers_raw = fetch_json(f"{TM_CEAPI}/transferHistory/list/{tm_id}")
    transfers = []
    for item in transfers_raw.get("transfers", []):
        fee_text = item.get("fee", "")
        transfers.append({
            "type": "transfer",
            "date": item.get("date"),
            "season": item.get("season"),
            "from_club": item.get("from", {}).get("clubName"),
            "to_club": item.get("to", {}).get("clubName"),
            "fee_text": fee_text,
            "fee_value": clean_market_value(fee_text),
            "transfer_type": classify_transfer_type(fee_text),
        })

    # Market value via CEAPI
    mv_raw = fetch_json(f"{TM_CEAPI}/marketValueDevelopment/graph/{tm_id}")
    market_values = []
    for item in mv_raw.get("list", []):
        market_values.append({
            "type": "market_value",
            "date": item.get("datum_mw") or item.get("date"),
            "value": clean_market_value(item.get("mw") or item.get("value", "")),
            "club": item.get("verein") or item.get("club"),
        })

    # Injuries via HTML
    soup = fetch(f"{TM_BASE}/-/verletzungen/spieler/{tm_id}")
    injuries = []
    for tr in soup.select("table.items tbody tr"):
        tds = [td.text.strip() for td in tr.find_all("td")]
        if len(tds) < 4:
            continue
        injuries.append({
            "type": "injury",
            "season": tds[0] if tds else None,
            "injury": tds[1] if len(tds) > 1 else None,
            "from_date": tds[2] if len(tds) > 2 else None,
            "until_date": tds[3] if len(tds) > 3 else None,
            "days": parse_int(tds[4]) if len(tds) > 4 else None,
        })

    data = {
        "tm_id": tm_id,
        "transfers": transfers,
        "market_values": market_values,
        "injuries": injuries,
    }

    cache_set(ck, data)
    return ok(data, "tm")


# ─────────────────────────────────────────
# CLUB ENDPOINTS
# ─────────────────────────────────────────

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


@router.get("/club/{tm_id}/profile")
def club_profile(tm_id: str):
    """Uitgebreid clubprofiel: naam, stadion, coach, capaciteit, eigenaar, stichting, form, marktwaarde."""
    ck = cache_key("tm", "club_profile_v39", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/-/datenfakten/verein/{tm_id}")

    name_tag = soup.select_one("h1.data-header__headline-wrapper") or soup.find("h1")

    info = {}
    for tr in soup.select("table.profilheader tr"):
        tds = tr.find_all("td")
        if len(tds) == 2:
            label = tds[0].text.strip().rstrip(":").lower()
            value = tds[1].text.strip()
            info[label] = value

    mv_tag = soup.select_one("a.data-header__market-value-wrapper")
    mv_text = mv_tag.text.strip() if mv_tag else None

    # Coach from header or info
    coach_tag = soup.select_one("span.data-header__coach a") or soup.select_one("a[href*='/trainer/']")
    coach_name = coach_tag.text.strip() if coach_tag else info.get("coach") or info.get("trainer")
    coach_tm_id = extract_id(coach_tag["href"], "/trainer/") if coach_tag else None

    # Average attendance
    avg_att_raw = info.get("average attendance") or info.get("zuschauerschnitt") or info.get("ø zuschauer")
    avg_attendance = parse_int(avg_att_raw)

    # League position from header
    pos_tag = soup.select_one("span.data-header__league-level") or soup.select_one("a.data-header__league-link")
    league_pos = pos_tag.text.strip() if pos_tag else None

    # Form last 5
    form = parse_form(soup)

    data = {
        "tm_id": tm_id,
        "name": clean_name(name_tag.text if name_tag else None),
        "stadium": info.get("stadium") or info.get("stadion"),
        "stadium_capacity": parse_int(info.get("seats") or info.get("capacity") or info.get("plätze")),
        "avg_attendance": avg_attendance,
        "founded": info.get("founded") or info.get("gründung"),
        "address": info.get("address") or info.get("adresse"),
        "website": info.get("website"),
        "coach_name": coach_name,
        "coach_tm_id": coach_tm_id,
        "league_position": league_pos,
        "form_last_5": form,
        "total_market_value": clean_market_value(mv_text),
        "total_market_value_text": mv_text,
        "last_updated": now_iso(),
    }

    cache_set(ck, data)
    return ok(data, "tm")


@router.get("/club/{tm_id}/transfers")
def club_transfers(tm_id: str, season_id: str = Query("2024")):
    """Transfers in en uit voor een club per seizoen."""
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
        rows.append({
            "game_id": game_id,
            "date": date_td.text.strip(),
            "competition": comp_a.text.strip() if comp_a else None,
            "competition_tm_id": extract_id(comp_a["href"], "/wettbewerb/") if comp_a else None,
            "home_club": home_a.text.strip() if home_a else None,
            "home_club_tm_id": extract_id(home_a["href"], "/verein/") if home_a else None,
            "away_club": away_a.text.strip() if away_a else None,
            "away_club_tm_id": extract_id(away_a["href"], "/verein/") if away_a else None,
            "result": result_a.text.strip() if result_a else None,
        })

    cache_set(ck, rows)
    return ok(rows, "tm")


@router.get("/club/{tm_id}/results")
def club_results(tm_id: str, limit: int = Query(10, ge=1, le=50)):
    """Recente uitslagen van een club."""
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
        rows.append({
            "game_id": game_id,
            "date": date_td.text.strip(),
            "competition": comp_a.text.strip() if comp_a else None,
            "competition_tm_id": extract_id(comp_a["href"], "/wettbewerb/") if comp_a else None,
            "home_club": home_a.text.strip() if home_a else None,
            "home_club_tm_id": extract_id(home_a["href"], "/verein/") if home_a else None,
            "away_club": away_a.text.strip() if away_a else None,
            "away_club_tm_id": extract_id(away_a["href"], "/verein/") if away_a else None,
            "result": result_a.text.strip() if result_a else None,
        })

    cache_set(ck, rows)
    return ok(rows, "tm")


@router.get("/club/{tm_id}/stats")
def club_stats(tm_id: str, season_id: str = Query("2024")):
    """Clubstatistieken: goals, clean sheets, form per seizoen."""
    ck = cache_key("tm", "club_stats", id=tm_id, season=season_id)
    cached = cache_get(ck, TTL_LIVE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/-/leistungsdaten/verein/{tm_id}/saison_id/{season_id}/plus/1")
    rows = []

    for tr in soup.select("table.items tbody tr"):
        tds = [td.text.strip() for td in tr.find_all("td")]
        if len(tds) < 5:
            continue
        comp_a = tr.select_one("td a[href*='/wettbewerb/']")
        rows.append({
            "competition": comp_a.text.strip() if comp_a else None,
            "competition_tm_id": extract_id(comp_a["href"], "/wettbewerb/") if comp_a else None,
            "games": parse_int(tds[2]) if len(tds) > 2 else None,
            "wins": parse_int(tds[3]) if len(tds) > 3 else None,
            "draws": parse_int(tds[4]) if len(tds) > 4 else None,
            "losses": parse_int(tds[5]) if len(tds) > 5 else None,
            "goals": parse_int(tds[6]) if len(tds) > 6 else None,
            "goals_conceded": parse_int(tds[7]) if len(tds) > 7 else None,
            "clean_sheets": parse_int(tds[8]) if len(tds) > 8 else None,
        })

    cache_set(ck, rows)
    return ok(rows, "tm")


@router.get("/club/{tm_id}/staff")
def club_staff(tm_id: str):
    """Technische staf van een club: coach, assistent-coaches, keeperstrainer etc."""
    ck = cache_key("tm", "club_staff", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/-/trainer/verein/{tm_id}")
    staff = []

    for tr in soup.select("table.items tbody tr"):
        person_a = tr.select_one("td.hauptlink a[href*='/trainer/']") or tr.select_one("td.hauptlink a")
        role_td = tr.select_one("td.zentriert") or tr.find_all("td")[1] if len(tr.find_all("td")) > 1 else None
        nat_img = tr.select_one("img.flaggenrahmen")

        if not person_a:
            continue

        staff.append({
            "name": clean_name(person_a.text),
            "tm_id": extract_id(person_a.get("href", ""), "/trainer/"),
            "role": role_td.text.strip() if role_td else None,
            "nationality": nat_img.get("title") if nat_img else None,
        })

    cache_set(ck, staff)
    return ok(staff, "tm")


@router.get("/club/{tm_id}/youth")
def club_youth(tm_id: str):
    """Jeugdspelers in de selectie met Aziatische nationaliteit."""
    ck = cache_key("tm", "club_youth", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/-/kader/verein/{tm_id}/saison_id/2025/plus/1")
    players = []

    for tr in soup.select("table.items tbody tr"):
        pid_a = tr.select_one("td.hauptlink a[href*='/spieler/']")
        if not pid_a:
            continue
        nat_imgs = tr.select("img.flaggenrahmen")
        nats = list(dict.fromkeys(img.get("title") for img in nat_imgs if img.get("title")))

        # Only include if has Asian nationality
        if not any(n in ASIAN_NATIONALITIES for n in nats):
            continue

        pid = extract_id(pid_a.get("href", ""), "/spieler/")
        mv_td = tr.select_one("td.rechts.hauptlink")
        age_td = tr.select_one("td.zentriert")

        players.append({
            "tm_id": pid,
            "name": clean_name(pid_a.text),
            "nationalities": nats,
            "age": parse_int(age_td.text.strip()) if age_td else None,
            "market_value": clean_market_value(mv_td.text.strip()) if mv_td else None,
        })

    cache_set(ck, players)
    return ok(players, "tm")


# ─────────────────────────────────────────
# COMPETITION ENDPOINTS
# ─────────────────────────────────────────

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
        date_td = tr.select_one("td.zentriert")
        rows.append({
            "game_id": extract_id(result_a["href"], "/spielbericht/") if result_a else None,
            "date": date_td.text.strip() if date_td else None,
            "home_club": home_a.text.strip() if home_a else None,
            "home_club_tm_id": extract_id(home_a["href"], "/verein/") if home_a else None,
            "away_club": away_a.text.strip() if away_a else None,
            "away_club_tm_id": extract_id(away_a["href"], "/verein/") if away_a else None,
            "result": result_a.text.strip() if result_a else None,
        })

    cache_set(ck, rows)
    return ok(rows, "tm")


@router.get("/competition/{comp_id}/top-scorers")
def competition_top_scorers(comp_id: str, season_id: str = Query("2024")):
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
        player_a = tr.select_one("td.hauptlink a[href*='/spieler/']")
        club_a = tr.select_one("td.no-border-links a[href*='/verein/']")
        nat_img = tr.select_one("img.flaggenrahmen")
        goals_td = tr.select_one("td.zentriert.hauptlink") or tds[-1]
        rows.append({
            "rank": parse_int(tds[0].text.strip()),
            "player_name": clean_name(player_a.text) if player_a else None,
            "player_tm_id": extract_id(player_a["href"], "/spieler/") if player_a else None,
            "nationality": nat_img.get("title") if nat_img else None,
            "club": club_a.text.strip() if club_a else None,
            "club_tm_id": extract_id(club_a["href"], "/verein/") if club_a else None,
            "goals": parse_int(goals_td.text.strip()),
        })

    cache_set(ck, rows)
    return ok(rows, "tm")


@router.get("/competition/{comp_id}/top-assists")
def competition_top_assists(comp_id: str, season_id: str = Query("2024")):
    """Assist ranglijst voor een competitie."""
    ck = cache_key("tm", "top_assists", comp=comp_id, season=season_id)
    cached = cache_get(ck, TTL_LIVE)
    if cached:
        return ok(cached, "tm", cached=True)

    slug = COMP_SLUG.get(comp_id, comp_id.lower())
    soup = fetch(f"{TM_BASE}/{slug}/vorlagenjaeger/wettbewerb/{comp_id}/saison_id/{season_id}")
    rows = []

    for tr in soup.select("table.items tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue
        player_a = tr.select_one("td.hauptlink a[href*='/spieler/']")
        club_a = tr.select_one("td.no-border-links a[href*='/verein/']")
        nat_img = tr.select_one("img.flaggenrahmen")
        assists_td = tr.select_one("td.zentriert.hauptlink") or tds[-1]
        rows.append({
            "rank": parse_int(tds[0].text.strip()),
            "player_name": clean_name(player_a.text) if player_a else None,
            "player_tm_id": extract_id(player_a["href"], "/spieler/") if player_a else None,
            "nationality": nat_img.get("title") if nat_img else None,
            "club": club_a.text.strip() if club_a else None,
            "club_tm_id": extract_id(club_a["href"], "/verein/") if club_a else None,
            "assists": parse_int(assists_td.text.strip()),
        })

    cache_set(ck, rows)
    return ok(rows, "tm")


@router.get("/competition/{comp_id}/market-values")
def competition_market_values(comp_id: str, season_id: str = Query("2024")):
    """Marktwaarde per club in een competitie."""
    ck = cache_key("tm", "comp_market_values", comp=comp_id, season=season_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    slug = COMP_SLUG.get(comp_id, comp_id.lower())
    soup = fetch(f"{TM_BASE}/{slug}/marktwertevergleich/wettbewerb/{comp_id}/saison_id/{season_id}")
    rows = []

    for tr in soup.select("table.items tbody tr"):
        tds = [td.text.strip() for td in tr.find_all("td")]
        if len(tds) < 3:
            continue
        club_a = tr.select_one("td.hauptlink a[href*='/verein/']")
        mv_td = tr.select_one("td.rechts.hauptlink") or tr.find_all("td")[-1]
        rows.append({
            "rank": parse_int(tds[0]),
            "club": club_a.text.strip() if club_a else None,
            "club_tm_id": extract_id(club_a["href"], "/verein/") if club_a else None,
            "squad_size": parse_int(tds[2]) if len(tds) > 2 else None,
            "total_market_value": clean_market_value(mv_td.text.strip()) if mv_td else None,
            "total_market_value_text": mv_td.text.strip() if mv_td else None,
            "avg_market_value": clean_market_value(tds[-1]) if tds else None,
        })

    cache_set(ck, rows)
    return ok(rows, "tm")


@router.get("/competition/{comp_id}/asian-players")
def competition_asian_players(comp_id: str, season_id: str = Query("2024")):
    """Alle Aziatische spelers in een competitie — direct content-klaar."""
    ck = cache_key("tm", "asian_players", comp=comp_id, season=season_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    slug = COMP_SLUG.get(comp_id, comp_id.lower())
    # Use TM's nationality filter — scan all clubs in competition
    soup = fetch(f"{TM_BASE}/{slug}/startseite/wettbewerb/{comp_id}/saison_id/{season_id}")
    club_ids = []
    seen_clubs = set()

    for a in soup.select("td.hauptlink a[href*='/startseite/verein/']"):
        cid = extract_id(a.get("href", ""), "/verein/")
        if cid and cid not in seen_clubs:
            seen_clubs.add(cid)
            club_ids.append(cid)

    players = []
    seen_players = set()

    for cid in club_ids:
        time.sleep(random.uniform(1.5, 2.5))
        try:
            csoup = fetch(f"{TM_BASE}/-/kader/verein/{cid}/saison_id/{season_id}/plus/1")
        except Exception:
            continue

        for tr in csoup.select("table.items tbody tr"):
            pid_a = tr.select_one("td.hauptlink a[href*='/spieler/']")
            if not pid_a:
                continue
            nat_imgs = tr.select("img.flaggenrahmen")
            nats = list(dict.fromkeys(img.get("title") for img in nat_imgs if img.get("title")))
            if not any(n in ASIAN_NATIONALITIES for n in nats):
                continue
            pid = extract_id(pid_a.get("href", ""), "/spieler/")
            if pid in seen_players:
                continue
            seen_players.add(pid)
            mv_td = tr.select_one("td.rechts.hauptlink")
            pos_td = tr.select_one("td.posrela table td:last-child")
            players.append({
                "tm_id": pid,
                "name": clean_name(pid_a.text),
                "nationalities": nats,
                "position": pos_td.text.strip() if pos_td else None,
                "club_tm_id": cid,
                "market_value": clean_market_value(mv_td.text.strip()) if mv_td else None,
            })

    cache_set(ck, players)
    return ok(players, "tm")


@router.get("/competition/{comp_id}/form-table")
def competition_form_table(comp_id: str, season_id: str = Query("2024")):
    """Vorm-ranglijst op basis van laatste 5 wedstrijden per club."""
    ck = cache_key("tm", "form_table", comp=comp_id, season=season_id)
    cached = cache_get(ck, TTL_LIVE)
    if cached:
        return ok(cached, "tm", cached=True)

    slug = COMP_SLUG.get(comp_id, comp_id.lower())
    soup = fetch(f"{TM_BASE}/{slug}/formtabelle/wettbewerb/{comp_id}/saison_id/{season_id}")
    rows = []

    for tr in soup.select("table.items tbody tr"):
        tds = [td.text.strip() for td in tr.find_all("td")]
        if len(tds) < 4:
            continue
        club_a = tr.select_one("td.no-border-links.hauptlink a")
        # Extract form badges W/D/L
        form_spans = tr.select("span.greenBg, span.yellowBg, span.redBg, td.form span")
        form = []
        for span in form_spans:
            t = span.text.strip().upper()
            if t in ("W", "D", "L", "S", "U", "N"):
                # Normalize German (S=Sieg/W, U=Unentschieden/D, N=Niederlage/L)
                form.append({"S": "W", "U": "D", "N": "L"}.get(t, t))
        rows.append({
            "position": parse_int(tds[0]),
            "club": club_a.text.strip() if club_a else None,
            "club_tm_id": extract_id(club_a["href"], "/verein/") if club_a else None,
            "form_last_5": form[-5:],
            "points_last_5": parse_int(tds[-1]) if tds else None,
        })

    cache_set(ck, rows)
    return ok(rows, "tm")


# ─────────────────────────────────────────
# COMPETITIONS & CLUBS META
# ─────────────────────────────────────────

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


# ─────────────────────────────────────────
# MATCH ENDPOINT
# ─────────────────────────────────────────

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
# H2H ENDPOINT
# ─────────────────────────────────────────

@router.get("/h2h")
def head_to_head(home_id: str = Query(...), away_id: str = Query(...)):
    """Head-to-head geschiedenis tussen twee clubs."""
    ck = cache_key("tm", "h2h", home=home_id, away=away_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/-/head2head/verein/{home_id}/gegen/verein/{away_id}")

    # Summary stats
    summary_tds = soup.select("div.h2h-summary td, table.h2h td")
    home_wins = away_wins = draws = total = None

    stats_boxes = soup.select("div.box-content div.h2h-result, div.h2h-box")
    for box in stats_boxes:
        text = box.text.strip().lower()
        val = parse_int(re.search(r"\d+", text).group(0) if re.search(r"\d+", text) else "")
        if "win" in text and "home" in text:
            home_wins = val
        elif "win" in text and "away" in text:
            away_wins = val
        elif "draw" in text:
            draws = val

    # Recent matches
    matches = []
    for tr in soup.select("table.items tbody tr"):
        tds = [td.text.strip() for td in tr.find_all("td")]
        if len(tds) < 4:
            continue
        result_a = tr.select_one("a.ergebnis-link, td.ergebnis-link a")
        game_id = extract_id(result_a["href"], "/spielbericht/") if result_a else None
        matches.append({
            "game_id": game_id,
            "date": tds[0] if tds else None,
            "competition": tds[1] if len(tds) > 1 else None,
            "home_club": tds[2] if len(tds) > 2 else None,
            "away_club": tds[3] if len(tds) > 3 else None,
            "result": result_a.text.strip() if result_a else None,
        })

    data = {
        "home_club_tm_id": home_id,
        "away_club_tm_id": away_id,
        "total_matches": total,
        "home_wins": home_wins,
        "away_wins": away_wins,
        "draws": draws,
        "recent_matches": matches[:10],
    }

    cache_set(ck, data)
    return ok(data, "tm")


# ─────────────────────────────────────────
# CROSS-DATA: ASIAN PLAYERS GLOBAL
# ─────────────────────────────────────────

@router.get("/asian-players")
def asian_players_by_competition(comp_id: str = Query(...)):
    """Shortcut: alle Aziatische spelers in een competitie. Alias voor /competition/{id}/asian-players."""
    return competition_asian_players(comp_id=comp_id)
