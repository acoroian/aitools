"""Celery tasks — wired to the pipeline modules."""

import logging

from pipeline.celery_app import app

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


@app.task(bind=True, max_retries=2, default_retry_delay=600, name="pipeline.tasks.ingest_hcris_hha")
def ingest_hcris_hha(self):  # type: ignore[no-untyped-def]
    """Download and upsert CMS HCRIS Home Health Agency cost reports."""
    try:
        from pipeline.ingest.hcris import run
        result = run("hha")
        log.info("HCRIS HHA ingest complete: %s", result)
        return result
    except Exception as exc:
        log.exception("HCRIS HHA ingest failed: %s", exc)
        raise self.retry(exc=exc)


@app.task(
    bind=True,
    max_retries=2,
    default_retry_delay=600,
    name="pipeline.tasks.ingest_hcris_hospice",
)
def ingest_hcris_hospice(self):  # type: ignore[no-untyped-def]
    """Download and upsert CMS HCRIS Hospice cost reports."""
    try:
        from pipeline.ingest.hcris import run
        result = run("hospice")
        log.info("HCRIS Hospice ingest complete: %s", result)
        return result
    except Exception as exc:
        log.exception("HCRIS Hospice ingest failed: %s", exc)
        raise self.retry(exc=exc)


@app.task(bind=True, max_retries=2, default_retry_delay=600, name="pipeline.tasks.ingest_hcai")
def ingest_hcai(self):  # type: ignore[no-untyped-def]
    """Download and upsert CA HCAI annual SNF financial disclosure data."""
    try:
        from pipeline.ingest.hcai import run
        result = run()
        log.info("HCAI ingest complete: %s", result)
        return result
    except Exception as exc:
        log.exception("HCAI ingest failed: %s", exc)
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


@app.task(
    bind=True,
    max_retries=3,
    default_retry_delay=600,
    name="pipeline.tasks.ingest_cms_nh_compare",
)
def ingest_cms_nh_compare(self):  # type: ignore[no-untyped-def]
    """Download and upsert CMS Nursing Home Health Deficiencies (SNF, monthly)."""
    try:
        from pipeline.ingest.cms_nh_compare import run
        result = run()
        log.info("CMS NH Compare ingest complete: %s", result)
        refresh_violation_rollup.delay()
        return result
    except Exception as exc:
        log.exception("CMS NH Compare ingest failed: %s", exc)
        raise self.retry(exc=exc)


@app.task(
    bind=True,
    max_retries=3,
    default_retry_delay=600,
    name="pipeline.tasks.ingest_cdph_sea",
)
def ingest_cdph_sea(self):  # type: ignore[no-untyped-def]
    """Download and upsert CDPH State Enforcement Actions (CA, annual)."""
    try:
        from pipeline.ingest.cdph_sea import run
        result = run()
        log.info("CDPH SEA ingest complete: %s", result)
        refresh_violation_rollup.delay()
        return result
    except Exception as exc:
        log.exception("CDPH SEA ingest failed: %s", exc)
        raise self.retry(exc=exc)


@app.task(
    bind=True,
    max_retries=2,
    default_retry_delay=60,
    name="pipeline.tasks.refresh_violation_rollup",
)
def refresh_violation_rollup(self):  # type: ignore[no-untyped-def]
    """Rebuild facility_violation_rollup from facility_violations."""
    try:
        from pipeline.db import get_session
        from pipeline.violations.rollup import refresh_violation_rollup as _refresh

        with get_session() as session:
            result = _refresh(session)
            session.commit()
        log.info("Violation rollup refresh complete: %s", result)
        return result
    except Exception as exc:
        log.exception("Violation rollup refresh failed: %s", exc)
        raise self.retry(exc=exc)
