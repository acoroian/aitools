"""Celery tasks — wired to the pipeline modules."""

import logging

from pipeline.celery_app import app
from pipeline.config import settings

log = logging.getLogger(__name__)


@app.task(bind=True, max_retries=3, default_retry_delay=300, name="pipeline.tasks.ingest_cdph")
def ingest_cdph(self):  # type: ignore[no-untyped-def]
    """Download and upsert CA CDPH Healthcare Facility Locations."""
    try:
        from pipeline.ingest.cdph import run
        result = run()
        log.info("CDPH ingest complete: %s", result)
        return result
    except Exception as exc:
        log.exception("CDPH ingest failed: %s", exc)
        raise self.retry(exc=exc)


@app.task(bind=True, max_retries=3, default_retry_delay=300, name="pipeline.tasks.ingest_crosswalk")
def ingest_crosswalk(self):  # type: ignore[no-untyped-def]
    """Apply the CA Licensed Facility Crosswalk (CDPH ↔ CMS NPI ↔ OSHPD)."""
    try:
        from pipeline.crosswalk.resolver import run
        result = run()
        log.info("Crosswalk apply complete: %s", result)
        return result
    except Exception as exc:
        log.exception("Crosswalk ingest failed: %s", exc)
        raise self.retry(exc=exc)


@app.task(bind=True, max_retries=2, default_retry_delay=60, name="pipeline.tasks.generate_tiles")
def generate_tiles(self, layer_slug: str):  # type: ignore[no-untyped-def]
    """Generate PMTiles for a single layer and save to TILES_DIR."""
    try:
        from pipeline.tiles.generate import run
        result = run(layer_slug)
        log.info("Tile generation complete for %s: %s", layer_slug, result)
        return result
    except Exception as exc:
        log.exception("Tile generation failed for %s: %s", layer_slug, exc)
        raise self.retry(exc=exc)


@app.task(name="pipeline.tasks.generate_all_tiles")
def generate_all_tiles():  # type: ignore[no-untyped-def]
    """Regenerate PMTiles for all known layers."""
    from pipeline.db import get_session
    from pipeline.models import Layer

    with get_session() as session:
        slugs = [row.slug for row in session.query(Layer.slug).all()]

    results = {}
    for slug in slugs:
        try:
            result = generate_tiles.delay(slug)
            results[slug] = str(result.id)
        except Exception as exc:
            log.error("Failed to queue tile generation for %s: %s", slug, exc)
            results[slug] = "error"

    return results
