"""
POST /facilities/filter — spatial + attribute filter returning GeoJSON.

All SQL uses SQLAlchemy text() with named parameters — no f-string interpolation.
The spatial polygon filter uses ST_Intersects against the PostGIS geometry column.
"""

import json
import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.db import get_db
from api.schemas import FacilityFilterRequest, GeoJSONFeature, GeoJSONFeatureCollection

log = logging.getLogger(__name__)
router = APIRouter()


@router.post("/filter", response_model=GeoJSONFeatureCollection)
def filter_facilities(
    req: FacilityFilterRequest,
    db: Session = Depends(get_db),
) -> GeoJSONFeatureCollection:
    """
    Return facilities as GeoJSON matching the given attribute + spatial filters.
    The client uses this to highlight features already rendered in the tile layer.
    """
    params: dict[str, object] = {"limit": req.limit, "offset": req.offset}
    where_clauses: list[str] = ["f.geom IS NOT NULL"]

    # ── Attribute filters ─────────────────────────────────────────────────────

    if req.facility_types:
        params["facility_types"] = req.facility_types
        where_clauses.append("f.type = ANY(:facility_types)")

    if req.license_status:
        params["license_status"] = req.license_status
        where_clauses.append("f.license_status = :license_status")

    if req.county:
        params["county"] = req.county.title()
        where_clauses.append("f.county = :county")

    if req.certified_medicare is not None:
        params["certified_medicare"] = req.certified_medicare
        where_clauses.append("f.certified_medicare = :certified_medicare")

    if req.certified_medicaid is not None:
        params["certified_medicaid"] = req.certified_medicaid
        where_clauses.append("f.certified_medicaid = :certified_medicaid")

    # Financial filters require a join
    needs_financials = any([
        req.gross_revenue_min is not None,
        req.gross_revenue_max is not None,
        req.year is not None,
    ])

    fin_join = ""
    if needs_financials:
        year_filter = "AND fin_sub.year = :fin_year" if req.year else ""
        if req.year:
            params["fin_year"] = req.year
        fin_join = f"""
            LEFT JOIN LATERAL (
                SELECT gross_revenue, year
                FROM facility_financials
                WHERE facility_id = f.id {year_filter}
                ORDER BY year DESC
                LIMIT 1
            ) fin ON true
        """
        if req.gross_revenue_min is not None:
            params["rev_min"] = req.gross_revenue_min
            where_clauses.append("COALESCE(fin.gross_revenue, 0) >= :rev_min")
        if req.gross_revenue_max is not None:
            params["rev_max"] = req.gross_revenue_max
            where_clauses.append("COALESCE(fin.gross_revenue, 0) <= :rev_max")
    else:
        fin_join = """
            LEFT JOIN LATERAL (
                SELECT gross_revenue, year
                FROM facility_financials
                WHERE facility_id = f.id
                ORDER BY year DESC
                LIMIT 1
            ) fin ON true
        """

    # Violation count filter
    if req.violation_count_max is not None:
        params["viol_max"] = req.violation_count_max
        where_clauses.append("""
            (SELECT COUNT(*) FROM facility_violations WHERE facility_id = f.id) <= :viol_max
        """)

    # ── Spatial filter ────────────────────────────────────────────────────────
    if req.spatial:
        try:
            geojson_str = json.dumps({"type": req.spatial.type, "coordinates": req.spatial.coordinates})
        except Exception as exc:
            raise HTTPException(status_code=422, detail=f"Invalid spatial filter: {exc}") from exc
        params["polygon"] = geojson_str
        where_clauses.append("ST_Intersects(f.geom, ST_SetSRID(ST_GeomFromGeoJSON(:polygon), 4326))")

    # ── Build query ───────────────────────────────────────────────────────────
    where_sql = " AND ".join(where_clauses)

    query = text(f"""
        SELECT
            f.id::text,
            f.name,
            f.type,
            f.subtype,
            f.address,
            f.city,
            f.county,
            f.zip,
            f.license_status,
            f.certified_medicare,
            f.certified_medicaid,
            f.lat,
            f.lon,
            COALESCE(fin.gross_revenue, 0) AS gross_revenue,
            fin.year AS revenue_year,
            (SELECT COUNT(*)::int FROM facility_violations WHERE facility_id = f.id) AS violation_count,
            (SELECT MAX(survey_date)::text FROM facility_violations WHERE facility_id = f.id) AS last_violation
        FROM facilities f
        {fin_join}
        WHERE {where_sql}
        ORDER BY f.name
        LIMIT :limit OFFSET :offset
    """)  # noqa: S608

    rows = db.execute(query, params).fetchall()

    features = [
        GeoJSONFeature(
            geometry={"type": "Point", "coordinates": [r[13], r[12]]} if r[12] and r[13] else {"type": "Point", "coordinates": []},  # lon, lat
            properties={
                "id": r[0],
                "name": r[1],
                "type": r[2],
                "subtype": r[3],
                "address": r[4],
                "city": r[5],
                "county": r[6],
                "zip": r[7],
                "license_status": r[8],
                "certified_medicare": r[9],
                "certified_medicaid": r[10],
                "gross_revenue": r[13],
                "revenue_year": r[14],
                "violation_count": r[15],
                "last_violation": r[16],
            },
        )
        for r in rows
        if r[12] and r[13]  # skip features with no coordinates
    ]

    return GeoJSONFeatureCollection(features=features, total=len(features))


@router.get("/{facility_id}")
def get_facility(facility_id: str, db: Session = Depends(get_db)) -> dict:
    """Return full facility record including all financials and violations."""
    row = db.execute(
        text("""
            SELECT id::text, name, type, subtype, address, city, county, zip, phone,
                   license_status, license_number, license_expiry::text,
                   certified_medicare, certified_medicaid, lat, lon,
                   cdph_id, cms_npi, oshpd_id, cdss_id
            FROM facilities WHERE id = :id
        """),
        {"id": facility_id},
    ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Facility not found")

    financials = db.execute(
        text("""
            SELECT year, source, gross_revenue, net_revenue, total_expenses,
                   medicare_revenue, medicaid_revenue, private_revenue, total_visits, total_patients
            FROM facility_financials WHERE facility_id = :id ORDER BY year DESC
        """),
        {"id": facility_id},
    ).fetchall()

    violations = db.execute(
        text("""
            SELECT survey_date::text, source, deficiency_tag, category, severity,
                   scope, description, resolved, resolved_date::text
            FROM facility_violations WHERE facility_id = :id ORDER BY survey_date DESC
        """),
        {"id": facility_id},
    ).fetchall()

    return {
        "id": row[0], "name": row[1], "type": row[2], "subtype": row[3],
        "address": row[4], "city": row[5], "county": row[6], "zip": row[7], "phone": row[8],
        "license_status": row[9], "license_number": row[10], "license_expiry": row[11],
        "certified_medicare": row[12], "certified_medicaid": row[13],
        "lat": row[14], "lon": row[15],
        "cdph_id": row[16], "cms_npi": row[17], "oshpd_id": row[18], "cdss_id": row[19],
        "financials": [
            {
                "year": f[0], "source": f[1], "gross_revenue": f[2], "net_revenue": f[3],
                "total_expenses": f[4], "medicare_revenue": f[5], "medicaid_revenue": f[6],
                "private_revenue": f[7], "total_visits": f[8], "total_patients": f[9],
            }
            for f in financials
        ],
        "violations": [
            {
                "survey_date": v[0], "source": v[1], "deficiency_tag": v[2],
                "category": v[3], "severity": v[4], "scope": v[5],
                "description": v[6], "resolved": v[7], "resolved_date": v[8],
            }
            for v in violations
        ],
    }
