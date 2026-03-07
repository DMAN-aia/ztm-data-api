# ZTM Data API

Football data aggregator for ZoomtheMatch. One service, five sources.

**Base URL:** `https://ztm-data-api.onrender.com`

---

## Sources

| Prefix | Source | Method | Best for |
|---|---|---|---|
| `/fbref` | FBref (StatHead) | HTTP + BS4 | xG, passing, pressing, deep stats |
| `/whoscored` | WhoScored | Selenium | Match events, ratings, heatmaps |
| `/understat` | Understat | JSON from HTML | xG per shot, player xG/xA |
| `/sofascore` | Sofascore | Unofficial API | Live scores, schedule, H2H |
| `/tm` | Transfermarkt | HTTP + BS4 | Market values, transfers, profiles |

---

## League IDs

| Code | League |
|---|---|
| `GB1` | Premier League |
| `GB2` | Championship |
| `L1` | Bundesliga |
| `IT1` | Serie A |
| `FR1` | Ligue 1 |
| `NL1` | Eredivisie |
| `ES1` | La Liga |
| `CL` | UEFA Champions League |
| `EL` | UEFA Europa League |
| `MLS` | Major League Soccer |
| `SA` | Saudi Pro League |
| `AL` | A-League Men |
| `JP1` | J1 League |
| `KR1` | K League 1 |
| `TH1` | Thai League 1 |
| `VN1` | V.League 1 |
| `MY1` | Super League Malaysia |

---

## Endpoints

### FBref
```
GET /fbref/schedule/{league_id}?season=2425
GET /fbref/player/season/{league_id}?season=2425&stat_type=standard
GET /fbref/team/season/{league_id}?season=2425&stat_type=shooting
GET /fbref/player/match/{league_id}?season=2425&stat_type=passing&match_id=optional
GET /fbref/team/match/{league_id}?season=2425&stat_type=schedule&team=optional
```
**stat_type options:** standard, shooting, passing, defense, possession, misc, keeper

### WhoScored
```
GET /whoscored/schedule/{league_id}?season=2425
GET /whoscored/events/{league_id}?season=2425&match_id=optional
```

### Understat
```
GET /understat/player/season/{league_id}?season=2425
GET /understat/player/match/{league_id}?season=2425
GET /understat/team/season/{league_id}?season=2425
GET /understat/shots/{league_id}?season=2425
```
**Supported leagues:** GB1, L1, IT1, FR1, ES1 only

### Sofascore
```
GET /sofascore/schedule/{league_id}?season=2425
GET /sofascore/standings/{league_id}?season=2425
```

### Transfermarkt
```
GET /tm/player/{tm_id}/profile
GET /tm/player/{tm_id}/transfers
GET /tm/player/{tm_id}/market-value
GET /tm/club/{tm_id}/squad
GET /tm/competitions/{comp_id}/standings
GET /tm/competitions/{comp_id}/matches?matchday=current|previous|next
```

---

## Response format

All endpoints return:
```json
{
  "status": "ok",
  "source": "fbref",
  "cached": false,
  "timestamp": "2026-03-07T12:00:00Z",
  "data": [...]
}
```

---

## Season codes

| Code | Season |
|---|---|
| `2425` | 2024/25 |
| `2324` | 2023/24 |
| `2526` | 2025/26 |

---

## Notes

- WhoScored endpoints are slower (Selenium, ~5-10s). Use for post-match event data only.
- Understat only covers top 5 European leagues + Russia.
- All responses are cached in `/tmp/soccerdata_cache`. Cache resets on dyno restart.
- Rate limiting is built into TM scraper (2-4s random delay per request).
