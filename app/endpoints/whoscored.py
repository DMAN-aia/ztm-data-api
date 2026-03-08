"""
WhoScored endpoints — tijdelijk niet beschikbaar.
WhoScored vereist browser automation (Selenium) die niet betrouwbaar werkt
op Render free tier. Geparkeerd tot alternatieve aanpak beschikbaar is.
"""

from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter()

NOT_AVAILABLE = JSONResponse(
    status_code=503,
    content={
        "status": "unavailable",
        "source": "whoscored",
        "detail": "WhoScored scraper is temporarily unavailable on this deployment.",
    }
)

@router.get("/schedule/{league_id}")
def schedule(league_id: str):
    return NOT_AVAILABLE

@router.get("/events/{league_id}")
def events(league_id: str):
    return NOT_AVAILABLE
