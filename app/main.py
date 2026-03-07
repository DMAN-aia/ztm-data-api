"""
ZTM Data API — main entry point
Sources: FBref, WhoScored, Understat, Sofascore (via soccerdata) + Transfermarkt (fork)
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.endpoints import fbref, whoscored, understat, sofascore, tm

app = FastAPI(
    title="ZTM Data API",
    description="Football data aggregator for ZoomtheMatch — FBref, WhoScored, Understat, Sofascore, Transfermarkt",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(fbref.router,      prefix="/fbref",      tags=["FBref"])
app.include_router(whoscored.router,  prefix="/whoscored",  tags=["WhoScored"])
app.include_router(understat.router,  prefix="/understat",  tags=["Understat"])
app.include_router(sofascore.router,  prefix="/sofascore",  tags=["Sofascore"])
app.include_router(tm.router,         prefix="/tm",         tags=["Transfermarkt"])

@app.get("/")
def root():
    return {
        "service": "ZTM Data API",
        "version": "1.0.0",
        "sources": ["fbref", "whoscored", "understat", "sofascore", "tm"],
        "status": "ok",
    }

@app.get("/health")
def health():
    return {"status": "ok"}
