from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from api.db import get_db
from api.schemas import LayerResponse

router = APIRouter()


@router.get("", response_model=list[LayerResponse])
def list_layers(db: Session = Depends(get_db)) -> list[LayerResponse]:
    from sqlalchemy import text

    rows = db.execute(
        text("""
            SELECT id::text, slug, name, description, facility_types,
                   min_zoom, max_zoom,
                   last_generated::text, record_count, bbox, access_policy
            FROM layers
            ORDER BY name
        """)
    ).fetchall()

    return [
        LayerResponse(
            id=r[0],
            slug=r[1],
            name=r[2],
            description=r[3],
            facility_types=r[4],
            min_zoom=r[5],
            max_zoom=r[6],
            last_generated=r[7],
            record_count=r[8],
            bbox=r[9],
            access_policy=r[10],
        )
        for r in rows
    ]
