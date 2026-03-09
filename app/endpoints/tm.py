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

TM_BASE = "https://www.transfermarkt.com"
TM_CEAPI = "https://www.transfermarkt.com/ceapi"

TTL_PROFILE = 86400
TTL_LIVE = 3600
TTL_MV = 43200

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.transfermarkt.com/",
}


def fetch(url: str):
    time.sleep(random.uniform(1.5, 3))
    r = requests.get(url, headers=HEADERS, timeout=20)

    if r.status_code == 403:
        raise HTTPException(status_code=403, detail="Transfermarkt blocked request")

    r.raise_for_status()

    return BeautifulSoup(r.text, "lxml")


def fetch_json(url: str):
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.json()


def extract_id(href: str, segment: str):
    if not href or segment not in href:
        return None
    try:
        return href.split(segment)[1].split("/")[0]
    except:
        return None


def t(s):
    return s.strip() if s else None


def now_iso():
    return datetime.utcnow().isoformat() + "Z"


def clean_market_value(raw: str):

    m = re.search(r"([€$£])([0-9,.]+)([mk]?)", raw or "", re.I)

    if not m:
        return {"value": None, "currency": None, "unit": None}

    return {
        "currency": m.group(1),
        "value": float(m.group(2).replace(",", "")),
        "unit": m.group(3).lower() or "unit",
    }


# ------------------------------------------------
# PLAYER PROFILE
# ------------------------------------------------

@router.get("/player/{tm_id}")
def player_profile(tm_id: str):

    ck = cache_key("tm", "profile_v22", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)

    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/-/profil/spieler/{tm_id}")

    name_tag = soup.find("h1")
    name = name_tag.text.strip() if name_tag else None

    club_tag = soup.select_one("span.data-header__club a")
    club_href = club_tag["href"] if club_tag else None

    nationalities = [
        img.get("title")
        for img in soup.select("img.flaggenrahmen")
        if img.get("title")
    ]

    data = {
        "tm_id": tm_id,
        "name": name,
        "nationalities": nationalities,
        "current_club": club_tag.text.strip() if club_tag else None,
        "club_tm_id": extract_id(club_href, "/verein/"),
        "last_updated": now_iso(),
    }

    cache_set(ck, data)

    return ok(data, "tm")


# ------------------------------------------------
# PLAYER STATS
# ------------------------------------------------

@router.get("/player/{tm_id}/stats")
def player_stats(tm_id: str, season_id: str = Query("2025")):

    ck = cache_key("tm", "stats_v22", id=tm_id, season=season_id)
    cached = cache_get(ck, TTL_PROFILE)

    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/-/leistungsdaten/spieler/{tm_id}/plus/0?saison={season_id}")

    rows = []

    for tr in soup.select("table.items tbody tr"):

        tds = tr.find_all("td")

        if len(tds) < 5:
            continue

        comp_a = tds[1].find("a")

        rows.append({
            "season": season_id,
            "competition": comp_a.text.strip() if comp_a else None,
            "competition_tm_id": extract_id(comp_a["href"], "/wettbewerb/") if comp_a else None,
            "appearances": tds[2].text.strip(),
            "goals": tds[3].text.strip(),
            "assists": tds[4].text.strip(),
        })

    cache_set(ck, rows)

    return ok(rows, "tm")


# ------------------------------------------------
# PLAYER TRANSFERS
# ------------------------------------------------

@router.get("/player/{tm_id}/transfers")
def player_transfers(tm_id: str):

    ck = cache_key("tm", "transfers_v22", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)

    if cached:
        return ok(cached, "tm", cached=True)

    data = fetch_json(f"{TM_CEAPI}/transferHistory/list/{tm_id}")

    transfers = []

    for tr in data.get("transfers", []):

        transfers.append({
            "season": tr.get("season"),
            "date": tr.get("date"),
            "from_club": tr.get("from", {}).get("clubName"),
            "to_club": tr.get("to", {}).get("clubName"),
        })

    cache_set(ck, transfers)

    return ok(transfers, "tm")


# ------------------------------------------------
# CLUB SQUAD
# ------------------------------------------------

@router.get("/club/{tm_id}/squad")
def club_squad(tm_id: str):

    ck = cache_key("tm", "squad_v22", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)

    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/-/kader/verein/{tm_id}/saison_id/2024")

    players = []

    for row in soup.select("table.items tbody tr"):

        name_a = row.select_one("td.hauptlink a")

        if not name_a:
            continue

        href = name_a["href"]

        players.append({
            "player_tm_id": extract_id(href, "/spieler/"),
            "name": name_a.text.strip(),
        })

    cache_set(ck, players)

    return ok(players, "tm")


# ------------------------------------------------
# HEALTH
# ------------------------------------------------

@router.get("/health")
def health():
    return {"status": "ok"}