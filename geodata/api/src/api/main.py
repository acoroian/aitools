import logging
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes import facilities, health, layers, tiles

logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="Geodata API",
    description="Care facility filter + tile API",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router, prefix="/health", tags=["health"])
app.include_router(layers.router, prefix="/layers", tags=["layers"])
app.include_router(facilities.router, prefix="/facilities", tags=["facilities"])
app.include_router(tiles.router, prefix="/tiles", tags=["tiles"])
