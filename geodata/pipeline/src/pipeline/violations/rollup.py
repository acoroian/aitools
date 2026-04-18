"""Rollup table refresh — one SQL transaction rebuilds the whole table."""

from __future__ import annotations

import logging
import time

from sqlalchemy import text
from sqlalchemy.orm import Session

log = logging.getLogger(__name__)

_TRUNCATE_SQL = "TRUNCATE facility_violation_rollup"

_REFRESH_SQL = text("""
INSERT INTO facility_violation_rollup (
    facility_id,
    violation_count_total, violation_count_12mo,
    cms_count_total, cms_count_12mo,
    cdph_count_total, cdph_count_12mo,
    max_severity_12mo, max_severity_level_12mo,
    has_ij_12mo,
    last_survey_date,
    last_refreshed_at
)
SELECT
    f.id AS facility_id,
    COUNT(v.id) AS violation_count_total,
    COUNT(v.id) FILTER (
        WHERE v.survey_date >= CURRENT_DATE - INTERVAL '12 months'
    ) AS violation_count_12mo,
    COUNT(v.id) FILTER (WHERE v.source = 'cms_nh_compare') AS cms_count_total,
    COUNT(v.id) FILTER (
        WHERE v.source = 'cms_nh_compare'
          AND v.survey_date >= CURRENT_DATE - INTERVAL '12 months'
    ) AS cms_count_12mo,
    COUNT(v.id) FILTER (WHERE v.source = 'cdph_sea') AS cdph_count_total,
    COUNT(v.id) FILTER (
        WHERE v.source = 'cdph_sea'
          AND v.survey_date >= CURRENT_DATE - INTERVAL '12 months'
    ) AS cdph_count_12mo,
    (
        SELECT v2.source || ':' || v2.severity
          FROM facility_violations v2
         WHERE v2.facility_id = f.id
           AND v2.survey_date >= CURRENT_DATE - INTERVAL '12 months'
         ORDER BY severity_level_ord(v2.source, v2.severity) DESC NULLS LAST
         LIMIT 1
    ) AS max_severity_12mo,
    MAX(severity_level_ord(v.source, v.severity))
        FILTER (WHERE v.survey_date >= CURRENT_DATE - INTERVAL '12 months')
        AS max_severity_level_12mo,
    COALESCE(
        BOOL_OR(is_immediate_jeopardy_sql(v.source, v.severity))
            FILTER (WHERE v.survey_date >= CURRENT_DATE - INTERVAL '12 months'),
        FALSE
    ) AS has_ij_12mo,
    MAX(v.survey_date) AS last_survey_date,
    NOW() AS last_refreshed_at
FROM facilities f
LEFT JOIN facility_violations v ON v.facility_id = f.id
GROUP BY f.id
""")


def refresh_violation_rollup(session: Session) -> dict[str, object]:
    """Rebuild facility_violation_rollup in a single transaction.

    Caller is responsible for commit/rollback — this lets callers
    compose the refresh with other work (e.g., the tests use a
    rollback-at-teardown session).

    Uses TRUNCATE + INSERT for speed and to avoid bloat; this acquires
    ACCESS EXCLUSIVE on ``facility_violation_rollup``, so the scheduler
    must run exactly one refresh at a time (Celery Beat, single worker).
    """
    start = time.monotonic()
    session.execute(text(_TRUNCATE_SQL))
    result = session.execute(_REFRESH_SQL)
    runtime = time.monotonic() - start
    count = result.rowcount
    log.info("refresh_violation_rollup: %d rows in %.2fs", count, runtime)
    return {"rows": int(count), "runtime_seconds": round(runtime, 3)}
