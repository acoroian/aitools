from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.db import get_db

router = APIRouter()


@router.get("")
def health_check(db: Session = Depends(get_db)) -> dict[str, str]:
    db.execute(text("SELECT PostGIS_Version()"))
    return {"status": "ok", "database": "connected"}
