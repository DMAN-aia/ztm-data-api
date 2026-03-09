@router.get("/player/{tm_id}")
def player_profile(tm_id: str):

    ck = cache_key("tm", "profile", id=tm_id)
    cached = cache_get(ck, TTL_PROFILE)

    if cached:
        return ok(cached, "tm", cached=True)

    try:

        soup = fetch(f"{TM_BASE}/-/profil/spieler/{tm_id}")

        name_tag = soup.select_one("h1")

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

    except Exception as e:

        raise HTTPException(status_code=500, detail=str(e))