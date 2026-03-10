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


def fetch(url: str):

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
        "last_updated": now_iso()
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