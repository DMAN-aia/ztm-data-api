# ONLY THE PARTS THAT CHANGED ARE THE URLS
# Everything else is identical to your file

TM_BASE     = "https://www.transfermarkt.com"
TM_CEAPI    = "https://www.transfermarkt.com/ceapi"

# -------------------------
# PLAYER PROFILE
# -------------------------

@router.get("/player/{tm_id}")
def player_profile(tm_id: str):

    ck = cache_key("tm", "profile_v22", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/profil/spieler/{tm_id}")   # FIXED


# -------------------------
# PLAYER STATS
# -------------------------

@router.get("/player/{tm_id}/stats")
def player_stats(tm_id: str, season_id: str = Query("2025")):

    ck = cache_key("tm", "stats_v27", id=tm_id, season=season_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/leistungsdaten/spieler/{tm_id}/plus/0?saison={season_id}")  # FIXED


# -------------------------
# PLAYER INJURIES
# -------------------------

@router.get("/player/{tm_id}/injuries")
def player_injuries(tm_id: str):

    ck = cache_key("tm", "injuries_v22", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/verletzungen/spieler/{tm_id}/plus/1")  # FIXED


# -------------------------
# PLAYER SUSPENSIONS
# -------------------------

@router.get("/player/{tm_id}/suspensions")
def player_suspensions(tm_id: str):

    ck = cache_key("tm", "suspensions_v22", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/sperrenhistorie/spieler/{tm_id}/plus/1")  # FIXED


# -------------------------
# NATIONAL TEAM
# -------------------------

@router.get("/player/{tm_id}/national-team")
def player_national_team(tm_id: str):

    ck = cache_key("tm", "national_v27", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/nationalmannschaft/spieler/{tm_id}")  # FIXED


# -------------------------
# CLUB SQUAD
# -------------------------

@router.get("/club/{tm_id}/squad")
def club_squad(tm_id: str):

    ck = cache_key("tm", "squad_v22", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)
    if cached:
        return ok(cached, "tm", cached=True)

    soup = fetch(f"{TM_BASE}/kader/verein/{tm_id}/saison_id/2024/plus/1")  # FIXED