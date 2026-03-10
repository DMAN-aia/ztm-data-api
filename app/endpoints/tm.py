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

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9"
}


def fetch(url: str):

    time.sleep(random.uniform(1.2, 2.4))

    r = requests.get(url, headers=HEADERS, timeout=25)

    if r.status_code == 403:
        raise HTTPException(status_code=403, detail="TM blocked request")

    r.raise_for_status()

    return BeautifulSoup(r.text, "lxml")


def fetch_json(url: str):

    r = requests.get(url, headers=HEADERS, timeout=25)

    r.raise_for_status()

    return r.json()


def extract_id(href: str, segment: str):

    if not href or segment not in href:
        return None

    try:
        return href.split(segment)[1].split("/")[0]
    except:
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
        return int(v)
    except:
        return None


def now_iso():
    return datetime.utcnow().isoformat() + "Z"


# PLAYER PROFILE

@router.get("/player/{tm_id}")
def player_profile(tm_id: str):

    ck = cache_key("tm", "profile_v50", id=tm_id)

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
        "last_updated": now_iso()
    }

    cache_set(ck, data)

    return ok(data, "tm")


# PLAYER STATS (stable parser)

@router.get("/player/{tm_id}/stats")
def player_stats(tm_id: str, season_id: str = Query("2025")):

    soup = fetch(f"{TM_BASE}/-/leistungsdaten/spieler/{tm_id}/plus/0?saison={season_id}")

    headers = [
        th.text.strip().lower()
        for th in soup.select("table.items thead th")
    ]

    rows = []

    for tr in soup.select("table.items tbody tr"):

        tds = [td.text.strip() for td in tr.find_all("td")]

        if len(tds) < 5:
            continue

        comp_a = tr.select_one("td:nth-child(2) a")

        row = {
            "season": season_id,
            "competition": comp_a.text.strip() if comp_a else None,
            "competition_tm_id": extract_id(comp_a["href"], "/wettbewerb/") if comp_a else None
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


# TRANSFERS

@router.get("/player/{tm_id}/transfers")
def player_transfers(tm_id: str):

    data = fetch_json(f"{TM_CEAPI}/transferHistory/list/{tm_id}")

    transfers = []

    for tr in data.get("transfers", []):

        transfers.append({
            "season": tr.get("season"),
            "date": tr.get("date"),
            "from_club": tr.get("from", {}).get("clubName"),
            "to_club": tr.get("to", {}).get("clubName")
        })

    return ok(transfers, "tm")


# MARKET VALUE

@router.get("/player/{tm_id}/market-value-history")
def market_value_history(tm_id: str):

    data = fetch_json(f"{TM_CEAPI}/marketValueDevelopment/graph/{tm_id}")

    return ok(data.get("list", []), "tm")


# INJURIES

@router.get("/player/{tm_id}/injuries")
def player_injuries(tm_id: str):

    soup = fetch(f"{TM_BASE}/-/verletzungen/spieler/{tm_id}")

    injuries = []

    for row in soup.select("table.items tbody tr"):

        tds = [td.text.strip() for td in row.find_all("td")]

        if len(tds) < 4:
            continue

        injuries.append({
            "season": tds[0],
            "injury_type": tds[1],
            "start_date": tds[2],
            "end_date": tds[3],
            "matches_missed": parse_int(tds[4]) if len(tds) > 4 else None
        })

    return ok(injuries, "tm")


# SUSPENSIONS

@router.get("/player/{tm_id}/suspensions")
def player_suspensions(tm_id: str):

    soup = fetch(f"{TM_BASE}/-/sperrenhistorie/spieler/{tm_id}")

    rows = []

    for row in soup.select("table.items tbody tr"):

        tds = [td.text.strip() for td in row.find_all("td")]

        if len(tds) < 3:
            continue

        rows.append({
            "competition": tds[0],
            "reason": tds[1],
            "matches_missed": parse_int(tds[2])
        })

    return ok(rows, "tm")


# NATIONAL TEAM

@router.get("/player/{tm_id}/national-team")
def national_team(tm_id: str):

    soup = fetch(f"{TM_BASE}/-/nationalmannschaft/spieler/{tm_id}")

    rows = []

    for tr in soup.select("table.items tbody tr"):

        tds = [td.text.strip() for td in tr.find_all("td")]

        if len(tds) < 5:
            continue

        rows.append({
            "competition": tds[1],
            "caps": parse_int(tds[2]),
            "goals": parse_int(tds[3]),
            "assists": parse_int(tds[4])
        })

    return ok(rows, "tm")