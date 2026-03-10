"""
ZTM Data API — Transfermarkt scraper
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.endpoints import tm

app = FastAPI(
    title="ZTM Data API",
    description="Football data voor ZoomtheMatch — Transfermarkt",
    version="6.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(tm.router, prefix="/tm", tags=["Transfermarkt"])

@app.get("/")
def root():
    return {"service": "ZTM Data API", "version": "6.0.0", "sources": ["tm"], "status": "ok"}

@app.get("/health")
def health():
    return {"status": "ok"}
