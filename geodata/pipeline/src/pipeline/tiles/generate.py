"""
Vector tile generation: PostGIS → GeoJSON → Tippecanoe → PMTiles.

For local dev the output is written to TILES_DIR (no R2 upload).
In production, pair with r2_upload.py to push the result to Cloudflare R2.
"""

import json
import logging
import os
import shutil
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import text

from pipeline.config import settings
from pipeline.db import get_session

log = logging.getLogger(__name__)

# SQL: export facilities as GeoJSON features, enriched with latest financials + violation rollup
EXPORT_SQL = """
SELECT json_build_object(
    'type', 'Feature',
    'geometry', ST_AsGeoJSON(f.geom)::json,
    'properties', json_build_object(
        'id',             f.id::text,
        'name',           f.name,
        'type',           f.type,
        'subtype',        f.subtype,
        'address',        f.address,
        'city',           f.city,
        'county',         f.county,
        'zip',            f.zip,
        'phone',          f.phone,
        'license_status', f.license_status,
        'certified_medicare', f.certified_medicare,
        'certified_medicaid', f.certified_medicaid,
        'gross_revenue',  COALESCE(fin.gross_revenue, 0),
        'revenue_year',   fin.year,
        'violation_count',        COALESCE(viol.violation_count_total, 0),
        'violation_count_12mo',   COALESCE(viol.violation_count_12mo, 0),
        'max_severity_level_12mo', COALESCE(viol.max_severity_level_12mo, 0),
        'has_ij_12mo',            COALESCE(viol.has_ij_12mo, FALSE),
        'last_survey_date',       viol.last_survey_date::text
    )
) AS feature
FROM facilities f
LEFT JOIN LATERAL (
    SELECT gross_revenue, year
    FROM facility_financials
    WHERE facility_id = f.id
    ORDER BY year DESC
    LIMIT 1
) fin ON true
LEFT JOIN facility_violation_rollup viol ON viol.facility_id = f.id
WHERE f.geom IS NOT NULL
{type_filter}
"""


def _export_geojson(layer_slug: str, output_path: str) -> int:
    """Export facilities from PostGIS to a GeoJSON FeatureCollection file."""
    from pipeline.db import get_session
    from pipeline.models import Layer

    with get_session() as session:
        layer = session.query(Layer).filter_by(slug=layer_slug).first()
        if not layer:
            raise ValueError(f"Layer not found: {layer_slug}")

        if layer.facility_types:
            type_filter = f"AND f.type = ANY(ARRAY{layer.facility_types!r})"
        else:
            type_filter = ""  # all-care layer

        sql = text(EXPORT_SQL.format(type_filter=type_filter))
        rows = session.execute(sql).fetchall()

    features = [row[0] for row in rows]
    collection = {"type": "FeatureCollection", "features": features}

    with open(output_path, "w") as f:
        json.dump(collection, f)

    log.info("Exported %d features to %s", len(features), output_path)
    return len(features)


def _run_tippecanoe(geojson_path: str, pmtiles_path: str, layer_slug: str) -> None:
    """Run Tippecanoe to convert GeoJSON to PMTiles."""
    if not shutil.which("tippecanoe"):
        raise RuntimeError("tippecanoe not found in PATH — install it: brew install tippecanoe")

    cmd = [
        "tippecanoe",
        f"--output={pmtiles_path}",
        "--force",
        "--minimum-zoom=4",
        "--maximum-zoom=14",
        "--no-feature-limit",
        "--no-tile-size-limit",
        f"--layer={layer_slug}",
        "--attribute-type=gross_revenue:int",
        "--attribute-type=violation_count:int",
        "--attribute-type=violation_count_12mo:int",
        "--attribute-type=max_severity_level_12mo:int",
        "--attribute-type=has_ij_12mo:bool",
        "--attribute-type=certified_medicare:bool",
        "--attribute-type=certified_medicaid:bool",
        geojson_path,
    ]

    log.info("Running tippecanoe: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)

    if result.returncode != 0:
        log.error("tippecanoe stderr: %s", result.stderr)
        raise RuntimeError(f"tippecanoe failed (exit {result.returncode}): {result.stderr[-500:]}")

    log.info("tippecanoe complete: %s", result.stderr[-200:] if result.stderr else "ok")


def _update_layer_record(layer_slug: str, pmtiles_path: str, record_count: int) -> None:
    """Update the layers table with the new PMTiles path and metadata."""
    from pipeline.models import Layer

    with get_session() as session:
        layer = session.query(Layer).filter_by(slug=layer_slug).first()
        if layer:
            layer.pmtiles_path = pmtiles_path
            layer.last_generated = datetime.now(UTC)
            layer.record_count = record_count


def run(layer_slug: str) -> dict[str, object]:
    """
    Full tile generation pipeline for one layer:
    PostGIS export → Tippecanoe → PMTiles written to TILES_DIR.
    """
    tiles_dir = Path(settings.tiles_dir)
    tiles_dir.mkdir(parents=True, exist_ok=True)

    pmtiles_path = str(tiles_dir / f"{layer_slug}.pmtiles")

    with tempfile.NamedTemporaryFile(suffix=".geojson", delete=False) as tmp:
        geojson_path = tmp.name

    try:
        record_count = _export_geojson(layer_slug, geojson_path)

        if record_count == 0:
            log.warning("No features for layer %s — skipping tile generation", layer_slug)
            return {"layer": layer_slug, "records": 0, "skipped": True}

        _run_tippecanoe(geojson_path, pmtiles_path, layer_slug)
        _update_layer_record(layer_slug, pmtiles_path, record_count)

        size_mb = os.path.getsize(pmtiles_path) / 1024 / 1024
        log.info("Generated %s (%.1f MB, %d features)", pmtiles_path, size_mb, record_count)

        return {
            "layer": layer_slug,
            "pmtiles_path": pmtiles_path,
            "records": record_count,
            "size_mb": round(size_mb, 2),
        }
    finally:
        if os.path.exists(geojson_path):
            os.unlink(geojson_path)
